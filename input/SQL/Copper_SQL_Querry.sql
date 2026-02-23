SELECT date, price, currency, source
FROM price_history
WHERE item_table = 'raw_materials'
  AND item_id = 13                     -- ← replace 5 with real copper id
  AND date >= '2016-01-01'
ORDER BY date ASC;