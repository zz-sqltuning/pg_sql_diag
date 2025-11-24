SELECT
        a_ptp_id,
        a_port_id,
        z_ptp_id,
        z_port_id,
        direction,
        p2.full_name full_name_pa,
        p3.full_name full_name_pz,
        p4.full_name full_name_ca,
        p5.full_name full_name_cz
        FROM
        teletop_1000.t_cross_connect p1
        LEFT JOIN teletop_1000.t_trans_port p2 ON p1.a_ptp_id = p2.port_id
        LEFT JOIN teletop_1000.t_trans_port p3 ON p1.z_ptp_id = p3.port_id
        LEFT JOIN teletop_1000.t_trans_port p4 ON p1.a_port_id = p4.port_id
        LEFT JOIN teletop_1000.t_trans_port p5 ON p1.z_port_id = p5.port_id
        WHERE p2.tp_type in(1,2) AND p3.tp_type in(1,2) AND p4.tp_type = 3 AND p5.tp_type = 3
        and a_ne_id = '0D38654F9AA1130CECAA77C29F0848CC'