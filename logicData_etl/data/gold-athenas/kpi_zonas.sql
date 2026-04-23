SELECT 
    zona,
    COUNT(id_pedido) as total_pedidos,
    SUM(monto) as ingresos_totales,
    AVG(monto) as ticket_promedio,
    -- Ejemplo de métrica de eficiencia (suponiendo que calculaste la columna en Gold)
    COUNT(CASE WHEN estado = 'ENTREGADO' THEN 1 END) * 100.0 / COUNT(*) as porcentaje_exito
FROM "logidata_gold_db"."resumen_operativo"
GROUP BY zona
ORDER BY ingresos_totales DESC