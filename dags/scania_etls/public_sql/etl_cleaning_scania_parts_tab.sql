DELETE
FROM scania_parts_tab
WHERE insertion_date = (SELECT MIN(insertion_date) FROM scania_parts_tab)
    OR insertion_date = (SELECT MAX(insertion_date) FROM scania_parts_tab);