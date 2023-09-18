-- Insert na tabela [description_prg_code] - atualizando com dados novos

INSERT INTO description_prg_code (
    prg,
    cc,
    dc,
    prg_description
)
SELECT
    pl.prg,
    pl.cc,
    pl.dc,
    'Sem Descrição' AS prg_description
FROM price_list pl
LEFT JOIN description_prg_code dpc
	ON pl.prg = dpc.prg
WHERE dpc.prg IS NULL
GROUP BY 1, 2, 3
ORDER BY pl.prg DESC;


-- Update na tabela [description_prg_code] - atualizando os campos de um prg já existente

UPDATE description_prg_code dpc
SET cc = pl.cc,
    dc = pl.dc
FROM price_list pl
WHERE dpc.prg = pl.prg;
