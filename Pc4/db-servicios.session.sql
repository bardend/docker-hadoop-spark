SELECT "TipoBien",
    COUNT(*) as cantidad
FROM bienes_servicios
GROUP BY "TipoBien";