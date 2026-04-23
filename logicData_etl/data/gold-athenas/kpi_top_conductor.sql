SELECT 
    conductor,
    COUNT(id_pedido) AS total_entregas,
    SUM(monto) as sum_valor_entrega
    -- Calculamos el porcentaje de entregas a tiempo
    --ROUND(CAST(COUNT(CASE WHEN hora_real <= hora_programada THEN 1 END) AS DOUBLE) / COUNT(*) * 100, 2) AS pct_on_time
    -- Tiempo promedio de desviación en minutos
   -- AVG(date_diff('minute', hora_programada, hora_real)) AS promedio_retraso_min
FROM "logidata_gold_db"."resumen_operativo"
WHERE estado = 'ENTREGADO'
GROUP BY conductor
ORDER BY 2 DESC
LIMIT 10;