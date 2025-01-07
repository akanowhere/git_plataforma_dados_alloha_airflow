    select 
        Id_Contrato,
        datediff(month, primeiro_uso,ultimo_uso) as tempo_uso_meses 
    from (
        SELECT DISTINCT
            min(to_date(s.createdAt, 'dd-MM-yyyy HH:mm:ss')) as primeiro_uso,
            max(to_date(s.createdAt, 'dd-MM-yyyy HH:mm:ss')) as ultimo_uso,
            e.contract_number Id_Contrato
        FROM gold.app.fato_sessions s
        LEFT JOIN gold.app.fato_events e ON e.session_id = s.sessions_id
        group by all
    ) a