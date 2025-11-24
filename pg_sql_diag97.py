import re
import sys
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import sqlparse

CONFIG_FILE = "db_config.json"


def load_db_config():
    if not os.path.isfile(CONFIG_FILE):
        print(f"❌ 错误：未找到数据库配置文件 '{CONFIG_FILE}'")
        print(f"👉 请创建 '{CONFIG_FILE}'，内容参考：")
        print("""
{
    "host": "your_host",
    "port": "5432",
    "dbname": "your_db",
    "user": "your_user",
    "password": "your_pass",
    "search_path": ["your_schema", "public"]
}
        """.strip())
        sys.exit(1)
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        required = ["host", "port", "dbname", "user", "password"]
        for key in required:
            if key not in config or not str(config[key]).strip():
                raise ValueError(f"配置项 '{key}' 缺失或为空")
        return config
    except json.JSONDecodeError as e:
        print(f"❌ 配置文件格式错误：{e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 加载配置失败：{e}")
        sys.exit(1)


def get_connection_and_search_path():
    db_config = load_db_config()
    conn = psycopg2.connect(**{k: v for k, v in db_config.items() if k != 'search_path'})

    if 'search_path' in db_config and isinstance(db_config['search_path'], list):
        search_path = [str(s).strip() for s in db_config['search_path'] if str(s).strip()]
        print(f"ℹ️  使用配置文件中的 search_path: {search_path}")
        return conn, search_path

    try:
        with conn.cursor() as cur:
            cur.execute("SHOW search_path;")
            raw_search_path = cur.fetchone()[0]
            parts = [part.strip() for part in raw_search_path.split(',')]
            cur.execute("SELECT current_user;")
            current_user = cur.fetchone()[0]

            resolved = []
            for sp in parts:
                if sp == '"$user"':
                    resolved.append(current_user)
                elif sp.startswith('"') and sp.endswith('"'):
                    resolved.append(sp[1:-1])
                else:
                    resolved.append(sp)
            print(f"ℹ️  从数据库读取 search_path: {resolved}")
            return conn, resolved
    except Exception as e:
        print(f"⚠️  无法获取 search_path，回退到 ['public']: {e}")
        return conn, ['public']


def extract_table_names(sql: str):
    sql_clean = re.sub(r'--.*', '', sql)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    tables = set()

    def normalize_identifier(token):
        token = token.strip()
        if not token:
            return None
        if token.startswith('"') and token.endswith('"'):
            return token[1:-1]
        return token.lower()

    patterns = [
        (r'\bUPDATE\s+((?:["\w]\w*\.?)*["\w]\w*)', 'update'),
        (r'\bDELETE\s+FROM\s+((?:["\w]\w*\.?)*["\w]\w*)', 'delete'),
        (r'\bINSERT\s+INTO\s+((?:["\w]\w*\.?)*["\w]\w*)', 'insert'),
        (r'\b(?:FROM|JOIN|USING)\s+((?:["\w]\w*\.?)*["\w]\w*)', 'from_join'),
    ]

    for pattern, _ in patterns:
        matches = re.findall(pattern, sql_clean, re.IGNORECASE)
        for full in matches:
            full = full.strip()
            parts = re.split(r'\.', full, maxsplit=1)
            if len(parts) == 1:
                tbl = normalize_identifier(parts[0])
                if tbl:
                    tables.add((None, tbl))
            else:
                sch = normalize_identifier(parts[0])
                tbl = normalize_identifier(parts[1])
                if sch and tbl:
                    tables.add((sch, tbl))
    return list(tables)


def safe_execute_query(conn, query, params=None):
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        return cur.fetchall()
    except psycopg2.errors.InFailedSqlTransaction:
        conn.rollback()
        cur = conn.cursor()
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()


def resolve_table_schema(conn, schema_hint, table, search_path):
    if schema_hint is not None:
        try:
            rows = safe_execute_query(
                conn,
                "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s;",
                (schema_hint, table)
            )
            if rows:
                return schema_hint, table
        except Exception:
            pass

    for sch in search_path:
        try:
            rows = safe_execute_query(
                conn,
                "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s;",
                (sch, table)
            )
            if rows:
                return sch, table
        except Exception:
            continue

    return 'public', table


def get_table_definition(conn, schema, table):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema, table))
        return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()


def is_partitioned_table(conn, schema, table):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT c.relkind 
            FROM pg_class c 
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = %s AND c.relname = %s;
        """, (schema, table))
        row = cur.fetchone()
        return row and row[0] == 'p'
    finally:
        cur.close()


# === 新增：通用索引列提取函数 ===
def extract_index_columns_from_def(index_def: str) -> str:
    """
    从 pg_get_indexdef 返回的字符串中提取索引列。
    示例输入: CREATE INDEX idx ON tbl USING btree (col1, col2 DESC NULLS LAST)
    输出: col1, col2 DESC NULLS LAST
    """
    # 匹配 ON schema.table (...) 或 ON table (...) 后的第一个括号内容
    match = re.search(r'\bON\s+\S+\s*\(([^)]+)\)', index_def, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # 如果上面没匹配到，尝试直接找 USING 后的括号
    match2 = re.search(r'\bUSING\s+\w+\s*\(([^)]+)\)', index_def, re.IGNORECASE)
    if match2:
        return match2.group(1).strip()
    return ''


def get_regular_table_indexes_with_size(conn, schema, table):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                i.relname AS index_name,
                am.amname AS index_method,
                pg_get_indexdef(i.oid) AS index_def_raw,
                pg_relation_size(i.oid) AS index_bytes
            FROM pg_class t
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_index ix ON ix.indrelid = t.oid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            WHERE n.nspname = %s AND t.relname = %s
            ORDER BY i.relname;
        """, (schema, table))
        indexes = []
        for row in cur.fetchall():
            r = dict(row)
            index_def = r['index_def_raw']
            cols = extract_index_columns_from_def(index_def)
            r['index_columns_str'] = cols
            r['is_primary'] = 'PRIMARY KEY' in index_def
            r['is_unique'] = ('UNIQUE' in index_def) or r['is_primary']
            indexes.append(r)
        return indexes
    except Exception as e:
        print(f"⚠️ 获取普通表索引失败: {e}")
        return []
    finally:
        cur.close()


def normalize_index_def(def_str: str) -> str:
    if not def_str:
        return ""
    match = re.search(r'\s+USING\s+\w+\s*(\(.*\))', def_str, re.IGNORECASE | re.DOTALL)
    if match:
        core = match.group(1).strip()
        core = re.sub(r'\s+', ' ', core)
        core = core.replace(' (', '(').replace('( ', '(')
        core = core.replace(' )', ')').replace(') ', ')')
        return core.lower()
    return def_str.lower().strip()


def get_parent_table_logical_indexes_with_core(conn, schema, table):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                i.relname AS logical_index_name,
                idx.indisprimary,
                idx.indisunique,
                am.amname AS index_method,
                ARRAY(
                    SELECT pg_get_indexdef(idx.indexrelid, k + 1, TRUE)
                    FROM generate_subscripts(idx.indkey, 1) AS k
                    ORDER BY k
                ) AS index_columns,
                pg_get_indexdef(i.oid) AS index_def_raw
            FROM pg_index idx
            JOIN pg_class i ON i.oid = idx.indexrelid
            JOIN pg_class t ON t.oid = idx.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            WHERE t.relname = %s AND n.nspname = %s
            ORDER BY idx.indisprimary DESC, idx.indisunique DESC, i.relname;
        """, (table, schema))
        result = []
        for row in cur.fetchall():
            d = dict(row)
            cols = d.get("index_columns", [])
            if isinstance(cols, list):
                d["index_columns_str"] = ", ".join(cols)
            d['index_def_normalized'] = normalize_index_def(d['index_def_raw'])
            result.append(d)
        return result
    finally:
        cur.close()


def get_all_physical_indexes_with_size(conn, schema, table):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        regclass_literal = f"{schema}.{table}"
        cur.execute("""
            SELECT
                n.nspname AS partition_schema,
                c.relname AS partition_name,
                ic.relname AS physical_index_name,
                pg_relation_size(ic.oid) AS index_bytes,
                am.amname AS index_method,
                pg_get_indexdef(ic.oid) AS index_def_raw
            FROM pg_partition_tree(%s::regclass) pt
            JOIN pg_class c ON c.oid = pt.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_index i ON i.indrelid = c.oid
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN pg_am am ON am.oid = ic.relam
            WHERE pt.isleaf = true
              AND ic.relkind = 'i'
            ORDER BY c.relname, ic.relname;
        """, (regclass_literal,))
        indexes = []
        for row in cur.fetchall():
            r = dict(row)
            r['index_columns_str'] = extract_index_columns_from_def(r['index_def_raw'])
            r['index_def_normalized'] = normalize_index_def(r['index_def_raw'])
            indexes.append(r)
        return indexes
    except Exception as e:
        print(f"⚠️ 获取物理索引详情失败: {e}")
        return []
    finally:
        cur.close()


def aggregate_logical_index_sizes(logical_indexes, physical_indexes):
    from collections import defaultdict

    logical_by_def = {}
    for li in logical_indexes:
        norm_def = li['index_def_normalized']
        if norm_def not in logical_by_def:
            logical_by_def[norm_def] = {
                'logical_index_name': li['logical_index_name'],
                'is_primary': li['indisprimary'],
                'is_unique': li['indisunique'],
                'index_method': li['index_method'],
                'index_columns_str': li['index_columns_str'],
                'index_def_raw': li['index_def_raw'],
                'total_bytes': 0,
            }

    unmatched_physical = []
    for pi in physical_indexes:
        norm_def = pi['index_def_normalized']
        if norm_def in logical_by_def:
            logical_by_def[norm_def]['total_bytes'] += pi['index_bytes']
        else:
            unmatched_physical.append(pi)

    result = []
    for info in logical_by_def.values():
        bytes_val = info['total_bytes']
        if bytes_val == 0:
            info['total_size'] = "0 bytes"
        else:
            units = ['bytes', 'kB', 'MB', 'GB', 'TB']
            from math import log, floor
            exp = min(floor(log(bytes_val, 1024)), len(units) - 1)
            info['total_size'] = f"{bytes_val / (1024 ** exp):.1f} {units[exp]}"
        result.append(info)

    if unmatched_physical:
        print(f"⚠️ 有 {len(unmatched_physical)} 个物理索引未匹配到任何逻辑索引，可能为孤立索引。")

    return result


def get_partition_definitions_recursive(conn, schema, table):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = """
        WITH RECURSIVE parent_table AS (
            SELECT 
                c.oid AS parent_oid,
                c.relname AS parent_name,
                n.nspname AS parent_schema
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relname = %s AND n.nspname = %s
        ), partitions AS (
            SELECT 
                i.inhparent AS parent_oid,
                i.inhrelid AS child_oid,
                1 AS level,
                ARRAY[parent.relname] AS root_path,
                parent.relname AS immediate_parent,
                child.relname AS child_name,
                cn.nspname AS child_schema,
                pg_get_expr(child.relpartbound, child.oid) AS partition_boundary
            FROM pg_inherits i
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_namespace cn ON child.relnamespace = cn.oid
            WHERE i.inhparent = (SELECT parent_oid FROM parent_table)
            UNION ALL
            SELECT 
                i.inhparent,
                i.inhrelid,
                p.level + 1,
                p.root_path || p.child_name,
                parent.relname AS immediate_parent,
                child.relname AS child_name,
                cn.nspname AS child_schema,
                pg_get_expr(child.relpartbound, child.oid) AS partition_boundary
            FROM pg_inherits i
            INNER JOIN partitions p ON i.inhparent = p.child_oid
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_namespace cn ON child.relnamespace = cn.oid
        )
        SELECT
            p.root_path[1] AS root_parent,
            p.immediate_parent AS parent_partition,
            p.child_name AS child_partition,
            p.child_schema,
            p.level AS hierarchy_level,
            array_to_string(p.root_path || p.child_name, ' → ') AS full_path,
            p.partition_boundary
        FROM partitions p
        ORDER BY p.root_path, p.level;
        """
        cur.execute(query, (table, schema))
        results = []
        for row in cur.fetchall():
            r = dict(row)
            r['partition_boundary'] = r.get('partition_boundary') or ''
            results.append(r)
        return results
    except Exception as e:
        print(f"⚠️ 执行递归分区查询失败: {e}")
        return []
    finally:
        cur.close()


def get_all_leaf_partitions_with_size(conn, schema, table):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        regclass_literal = f"{schema}.{table}"
        cur.execute("""
            SELECT
                n.nspname AS partition_schema,
                c.relname AS partition_name,
                pg_table_size(c.oid) AS table_bytes,
                pg_indexes_size(c.oid) AS indexes_bytes,
                pg_total_relation_size(c.oid) AS total_bytes
            FROM pg_partition_tree(%s::regclass) pt
            JOIN pg_class c ON c.oid = pt.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE pt.isleaf = true
            ORDER BY c.relname;
        """, (regclass_literal,))
        partitions = []
        for row in cur.fetchall():
            r = dict(row)
            for key in ['table_bytes', 'indexes_bytes', 'total_bytes']:
                bytes_val = r[key]
                if bytes_val == 0:
                    r[f"{key}_hr"] = "0 bytes"
                else:
                    units = ['bytes', 'kB', 'MB', 'GB', 'TB']
                    from math import log, floor
                    exp = min(floor(log(bytes_val, 1024)), len(units) - 1)
                    r[f"{key}_hr"] = f"{bytes_val / (1024 ** exp):.1f} {units[exp]}"
            partitions.append(r)
        return partitions
    except Exception as e:
        print(f"⚠️ 获取分区详情失败: {e}")
        return []
    finally:
        cur.close()


def get_partition_stats_per_leaf(conn, schema, table):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        regclass_literal = f"{schema}.{table}"
        cur.execute("""
            SELECT
                n.nspname AS partition_schema,
                c.relname AS partition_name,
                COALESCE(st.seq_scan, 0)::bigint AS seq_scan,
                COALESCE(st.idx_scan, 0)::bigint AS idx_scan,
                COALESCE(st.n_tup_ins, 0)::bigint AS n_tup_ins,
                COALESCE(st.n_tup_upd, 0)::bigint AS n_tup_upd,
                COALESCE(st.n_tup_del, 0)::bigint AS n_tup_del,
                COALESCE(st.n_live_tup, 0)::bigint AS n_live_tup,
                COALESCE(st.n_dead_tup, 0)::bigint AS n_dead_tup,
                st.last_vacuum,
                st.last_autovacuum,
                st.last_analyze,
                st.last_autoanalyze
            FROM pg_partition_tree(%s::regclass) pt
            JOIN pg_class c ON c.oid = pt.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid
            WHERE pt.isleaf = true
            ORDER BY c.relname;
        """, (regclass_literal,))
        stats_list = []
        for row in cur.fetchall():
            stats_list.append(dict(row))
        return stats_list
    except Exception as e:
        print(f"⚠️ 获取分区统计信息失败: {e}")
        return []
    finally:
        cur.close()


def get_partitioned_table_total_size(conn, schema, table):
    cur = conn.cursor()
    try:
        regclass_literal = f"{schema}.{table}"
        cur.execute("""
            SELECT
                COALESCE(SUM(pg_table_size(pt.relid)), 0) AS table_bytes,
                COALESCE(SUM(pg_indexes_size(pt.relid)), 0) AS indexes_bytes,
                COALESCE(SUM(pg_total_relation_size(pt.relid)), 0) AS total_bytes
            FROM pg_partition_tree(%s::regclass) pt
            WHERE pt.isleaf = true;
        """, (regclass_literal,))
        row = cur.fetchone()
        if row:
            return {
                'table_bytes': row[0],
                'indexes_bytes': row[1],
                'total_bytes': row[2]
            }
        return {'table_bytes': 0, 'indexes_bytes': 0, 'total_bytes': 0}
    except Exception as e:
        print(f"⚠️ 计算总大小失败: {e}")
        return {'table_bytes': 0, 'indexes_bytes': 0, 'total_bytes': 0}
    finally:
        cur.close()


def get_regular_table_total_size(conn, schema, table):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                pg_table_size(quote_ident(%s) || '.' || quote_ident(%s)),
                pg_indexes_size(quote_ident(%s) || '.' || quote_ident(%s)),
                pg_total_relation_size(quote_ident(%s) || '.' || quote_ident(%s));
        """, (schema, table, schema, table, schema, table))
        row = cur.fetchone()
        return {
            'table_bytes': row[0] if row else 0,
            'indexes_bytes': row[1] if row else 0,
            'total_bytes': row[2] if row else 0
        }
    finally:
        cur.close()


def get_single_table_stats(conn, schema, table, is_partitioned):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if is_partitioned:
            regclass_literal = f"{schema}.{table}"
            cur.execute("""
                WITH leaf_partitions AS (
                    SELECT pt.relid
                    FROM pg_partition_tree(%s::regclass) pt
                    WHERE pt.isleaf = true
                )
                SELECT
                    %s AS schemaname,
                    %s AS tablename,
                    COALESCE(SUM(st.seq_scan), 0)::bigint AS seq_scan,
                    COALESCE(SUM(st.idx_scan), 0)::bigint AS idx_scan,
                    COALESCE(SUM(st.n_tup_ins), 0)::bigint AS n_tup_ins,
                    COALESCE(SUM(st.n_tup_upd), 0)::bigint AS n_tup_upd,
                    COALESCE(SUM(st.n_tup_del), 0)::bigint AS n_tup_del,
                    COALESCE(SUM(st.n_live_tup), 0)::bigint AS n_live_tup,
                    COALESCE(SUM(st.n_dead_tup), 0)::bigint AS n_dead_tup,
                    MAX(st.last_vacuum) AS last_vacuum,
                    MAX(st.last_autovacuum) AS last_autovacuum,
                    MAX(st.last_analyze) AS last_analyze,
                    MAX(st.last_autoanalyze) AS last_autoanalyze
                FROM leaf_partitions lp
                LEFT JOIN pg_stat_user_tables st ON st.relid = lp.relid;
            """, (regclass_literal, schema, table))
            stat_row = cur.fetchone()
            if stat_row:
                return dict(stat_row)
        else:
            cur.execute("""
                SELECT
                    schemaname,
                    relname AS tablename,
                    seq_scan,
                    idx_scan,
                    n_tup_ins,
                    n_tup_upd,
                    n_tup_del,
                    n_live_tup,
                    n_dead_tup,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze,
                    last_autoanalyze
                FROM pg_stat_user_tables
                WHERE schemaname = %s AND relname = %s;
            """, (schema, table))
            stat_row = cur.fetchone()
            if stat_row:
                return dict(stat_row)
        return {
            'schemaname': schema,
            'tablename': table,
            'seq_scan': 0, 'idx_scan': 0,
            'n_tup_ins': 0, 'n_tup_upd': 0, 'n_tup_del': 0,
            'n_live_tup': 0, 'n_dead_tup': 0,
            'last_vacuum': None, 'last_autovacuum': None,
            'last_analyze': None, 'last_autoanalyze': None,
        }
    finally:
        cur.close()


def explain_sql_text(conn, sql: str, search_path: list):
    cur = conn.cursor()
    try:
        quoted_paths = [f'"{s}"' for s in search_path]
        set_search_path_sql = f"SET LOCAL search_path TO {', '.join(quoted_paths)};"
        cur.execute(set_search_path_sql)
        cur.execute(f"EXPLAIN {sql}")
        return "\n".join(row[0] for row in cur.fetchall())
    except psycopg2.errors.InFailedSqlTransaction:
        conn.rollback()
        cur = conn.cursor()
        quoted_paths = [f'"{s}"' for s in search_path]
        set_search_path_sql = f"SET LOCAL search_path TO {', '.join(quoted_paths)};"
        cur.execute(set_search_path_sql)
        cur.execute(f"EXPLAIN {sql}")
        return "\n".join(row[0] for row in cur.fetchall())
    except Exception as e:
        return f"ERROR during EXPLAIN: {str(e)}"
    finally:
        cur.close()


def human_readable_size(bytes_val):
    if bytes_val == 0:
        return "0 bytes"
    units = ['bytes', 'kB', 'MB', 'GB', 'TB']
    from math import log, floor
    exp = min(floor(log(bytes_val, 1024)), len(units) - 1)
    return f"{bytes_val / (1024 ** exp):.1f} {units[exp]}"


# === 关键修改：显式指定表格列顺序 ===
def list_of_dicts_to_html_table(data_list, title="", ordered_keys=None):
    if not data_list:
        return f"<h3>{title}</h3><p>No data</p>"
    html = f"<h3>{title}</h3>\n<table border='1' class='data-table'>\n"
    
    if ordered_keys is None:
        # 自动推断，但优先保证常见字段靠前
        first_row = data_list[0]
        all_keys = list(first_row.keys())
        preferred_order = [
            '分区名', '物理索引名', '索引名称', '是否为主键', '是否唯一',
            '索引方法', '索引列', '索引大小', '索引大小（字节）',
            '总大小', '总大小（字节）',
            '表数据大小', '索引大小', '总大小',
            'schemaname', 'tablename', 'estimated_row_count'
        ]
        ordered_keys = []
        for k in preferred_order:
            if k in all_keys and k not in ordered_keys:
                ordered_keys.append(k)
        for k in all_keys:
            if k not in ordered_keys:
                ordered_keys.append(k)

    html += "<thead><tr>" + "".join(f"<th>{k}</th>" for k in ordered_keys) + "</tr></thead>\n<tbody>\n"
    for row in data_list:
        html += "<tr>" + "".join(f"<td>{row.get(k, '') or ''}</td>" for k in ordered_keys) + "</tr>\n"
    html += "</tbody></table>\n"
    return html


def text_to_pre_html(text, title="Execution Plan"):
    return f"<h3>{title}</h3>\n<pre>{text}</pre>"


def generate_html_report(sql, explain_text, tables_info):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_sql = sqlparse.format(
        sql,
        reindent=True,
        keyword_case='upper',
        indent_width=4
    )
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>PostgreSQL SQL 分析报告</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f9f9f9; }}
            h1, h2, h3 {{ color: #2c3e50; }}
            .container {{ max-width: 1200px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
            pre {{ 
                background: #f4f4f4; 
                padding: 15px; 
                border-radius: 6px; 
                overflow-x: auto; 
                white-space: pre-wrap;
                font-family: Consolas, Monaco, monospace;
                font-size: 14px;
                line-height: 1.4;
            }}
            table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
            .data-table tr:hover {{ background-color: #f5f5f5; }}
            .error {{ color: red; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>PostgreSQL SQL 分析报告</h1>
            <p><strong>生成时间：</strong> {timestamp}</p>
            <h2>原始 SQL（已格式化）</h2>
            <pre>{formatted_sql}</pre>
    """

    html += text_to_pre_html(explain_text, "执行计划 (EXPLAIN)")

    html += "<h2>涉及的表详情</h2>"
    if not tables_info:
        html += "<p>未检测到任何表。</p>"
    else:
        for (schema, table), info in tables_info.items():
            full_name = f"{schema}.{table}"
            html += f"<h3>📊 表: <code>{full_name}</code></h3>"
            if 'error' in info:
                html += f"<p class='error'>❌ 获取表信息失败: {info['error']}</p>"
                continue

            is_partitioned = info.get('is_partitioned', False)

            if info.get('size_summary'):
                size_sum = info['size_summary']
                summary_display = {
                    'schemaname': schema,
                    'tablename': table,
                    '表数据大小（字节）': size_sum['table_bytes'],
                    '表数据大小': human_readable_size(size_sum['table_bytes']),
                    '索引总大小（字节）': size_sum['indexes_bytes'],
                    '索引总大小': human_readable_size(size_sum['indexes_bytes']),
                    '总大小（字节）': size_sum['total_bytes'],
                    '总大小': human_readable_size(size_sum['total_bytes']),
                }
                html += list_of_dicts_to_html_table([summary_display], "存储与估算行数（主表汇总)")

            if info.get('stats'):
                html += list_of_dicts_to_html_table([info['stats']], "完整统计信息（I/O 与 DML)")

            if info.get('columns'):
                html += list_of_dicts_to_html_table(info['columns'], "表结构")

            if not is_partitioned and info.get('regular_indexes'):
                idx_display = []
                for idx in info['regular_indexes']:
                    idx_display.append({
                        "索引名称": idx['index_name'],
                        "是否为主键": "是" if idx['is_primary'] else "否",
                        "是否唯一": "是" if idx['is_unique'] else "否",
                        "索引方法": idx['index_method'],
                        "索引列": idx['index_columns_str'],
                        "索引大小": human_readable_size(idx['index_bytes']),
                        "索引大小（字节）": idx['index_bytes'],
                    })
                html += list_of_dicts_to_html_table(
                    idx_display,
                    "普通表索引详情",
                    ordered_keys=["索引名称", "是否为主键", "是否唯一", "索引方法", "索引列", "索引大小", "索引大小（字节）"]
                )

            if is_partitioned and info.get('logical_indexes_with_size'):
                logical_display = []
                for idx in info['logical_indexes_with_size']:
                    logical_display.append({
                        "索引名称": idx['logical_index_name'],
                        "是否为主键": "是" if idx['is_primary'] else "否",
                        "是否唯一": "是" if idx['is_unique'] else "否",
                        "索引方法": idx['index_method'],
                        "索引列": idx['index_columns_str'],
                        "总大小": idx['total_size'],
                        "总大小（字节）": idx['total_bytes'],
                    })
                html += list_of_dicts_to_html_table(
                    logical_display,
                    "逻辑索引总大小（汇总所有分区）",
                    ordered_keys=["索引名称", "是否为主键", "是否唯一", "索引方法", "索引列", "总大小", "总大小（字节）"]
                )

            if is_partitioned and info.get('partition_details'):
                part_display = []
                for p in info['partition_details']:
                    part_display.append({
                        '分区名': p['partition_name'],
                        '表数据大小': p['table_bytes_hr'],
                        '索引大小': p['indexes_bytes_hr'],
                        '总大小': p['total_bytes_hr'],
                    })
                html += list_of_dicts_to_html_table(part_display, "物理分区详情（含索引）")

            if is_partitioned and info.get('physical_index_details'):
                idx_display = []
                for idx in info['physical_index_details']:
                    idx_display.append({
                        '分区名': idx['partition_name'],
                        '物理索引名': idx['physical_index_name'],
                        '索引方法': idx['index_method'],
                        '索引列': idx['index_columns_str'],
                        '索引大小': human_readable_size(idx['index_bytes']),
                        '索引大小（字节）': idx['index_bytes'],
                    })
                html += list_of_dicts_to_html_table(
                    idx_display,
                    "物理索引详情（明细，按分区）",
                    ordered_keys=["分区名", "物理索引名", "索引方法", "索引列", "索引大小", "索引大小（字节）"]
                )

            if is_partitioned and info.get('recursive_partition_definitions'):
                def_display = []
                for d in info['recursive_partition_definitions']:
                    def_display.append({
                        '层级': d['hierarchy_level'],
                        '完整路径': d['full_path'],
                        '父分区': d['parent_partition'],
                        '子分区': f"{d['child_schema']}.{d['child_partition']}",
                        '分区边界': d['partition_boundary'],
                    })
                html += list_of_dicts_to_html_table(def_display, "各分区定义（PARTITION OF ...，含多级嵌套）")

    html += """
        </div>
    </body>
    </html>
    """
    return html


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python pg_sql_diag_full_detail.py \"SELECT ... ;\"")
        print("  python pg_sql_diag_full_detail.py query.sql")
        sys.exit(1)

    arg = sys.argv[1].strip()

    if os.path.isfile(arg):
        with open(arg, "r", encoding="utf-8") as f:
            user_sql = f.read().strip()
        print(f"ℹ️  从文件 '{arg}' 读取 SQL")
    else:
        user_sql = arg
        print("ℹ️  从命令行参数读取 SQL")

    if not user_sql.strip().endswith(';'):
        user_sql += ';'

    conn, search_path = get_connection_and_search_path()
    print("✅ 数据库连接成功")

    try:
        tables = extract_table_names(user_sql)
        print(f"🔍 检测到涉及的表: {tables}")

        tables_info = {}

        for schema_hint, table in tables:
            resolved_schema, resolved_table = resolve_table_schema(conn, schema_hint, table, search_path)
            key = (resolved_schema, resolved_table)
            print(f"📌 处理表: {resolved_schema}.{resolved_table}")

            try:
                is_part = is_partitioned_table(conn, resolved_schema, resolved_table)
                columns = get_table_definition(conn, resolved_schema, resolved_table)
                stats = get_single_table_stats(conn, resolved_schema, resolved_table, is_part)

                if is_part:
                    size_summary = get_partitioned_table_total_size(conn, resolved_schema, resolved_table)
                    partition_details = get_all_leaf_partitions_with_size(conn, resolved_schema, resolved_table)
                    recursive_defs = get_partition_definitions_recursive(conn, resolved_schema, resolved_table)
                    logical_indexes = get_parent_table_logical_indexes_with_core(conn, resolved_schema, resolved_table)
                    physical_indexes = get_all_physical_indexes_with_size(conn, resolved_schema, resolved_table)
                    logical_with_size = aggregate_logical_index_sizes(logical_indexes, physical_indexes)

                    tables_info[key] = {
                        'is_partitioned': True,
                        'columns': columns,
                        'stats': stats,
                        'size_summary': size_summary,
                        'partition_details': partition_details,
                        'recursive_partition_definitions': recursive_defs,
                        'logical_indexes_with_size': logical_with_size,
                        'physical_index_details': physical_indexes,
                    }
                else:
                    size_summary = get_regular_table_total_size(conn, resolved_schema, resolved_table)
                    regular_indexes = get_regular_table_indexes_with_size(conn, resolved_schema, resolved_table)
                    tables_info[key] = {
                        'is_partitioned': False,
                        'columns': columns,
                        'stats': stats,
                        'size_summary': size_summary,
                        'regular_indexes': regular_indexes,
                    }

            except Exception as e:
                print(f"⚠️  获取表 {resolved_schema}.{resolved_table} 信息时出错: {e}")
                tables_info[key] = {'error': str(e)}

        explain_result = explain_sql_text(conn, user_sql, search_path)
        print("✅ EXPLAIN 执行完成")

        report_html = generate_html_report(user_sql, explain_result, tables_info)
        output_file = f"pg_sql_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report_html)

        print(f"✅ 报告已生成: {os.path.abspath(output_file)}")

    finally:
        conn.close()
        print("🔌 数据库连接已关闭")


if __name__ == "__main__":
    main()