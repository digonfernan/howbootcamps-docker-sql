-- Criação da tabela Billboard
CREATE TABLE PUBLIC."Billboard1" (
	"date" DATE NULL
	,"rank" int4 NULL
	,song VARCHAR(300) NULL
	,artist VARCHAR(300) NULL
	,"last-week" float8 NULL
	,"peak-rank" int4 NULL
	,"weeks-on-board" int4 NULL
	);

-- Exploração do dataset
-- Mostra as 10 primeiras linhas
SELECT t1."date"
	,t1."rank"
	,t1."song"
	,t1."artist"
	,t1."last-week"
	,t1."peak-rank"
	,t1."weeks-on-board"
FROM PUBLIC."Billboard1" AS t1 limit 10;

-- Conta o número de linhas
SELECT count(*) AS quantidade
FROM PUBLIC."Billboard1";

-- Teste busca Linkin Park
SELECT
	t1."rank"
	,t1."song"
	,t1."artist"
	,t1."peak-rank"
	,t1."weeks-on-board"
FROM PUBLIC."Billboard1" AS t1
where t1.artist = 'Linkin Park';

-- Teste filtro Quantidade de vezes das músicas no topo
SELECT t1."song"
	,t1."artist"
	,count(*) AS vezes_musica
FROM PUBLIC."Billboard1" AS t1
WHERE t1.artist = 'Linkin Park'
GROUP BY t1.song
	,t1.artist
ORDER BY vezes_musica DESC;

-- Quantidade de vezes dos artistas no topo
SELECT t1.artist
	,count(*) AS qtd_artist
FROM PUBLIC."Billboard1" AS t1
GROUP BY t1.artist
ORDER BY t1.artist;

-- Quantidade de vezes das musicas no topo
SELECT t1.song
	,count(*) AS qtd_song
FROM PUBLIC."Billboard1" AS t1
GROUP BY t1.song
ORDER BY t1.song;

-- Join das quantidades de artistas e musicas
SELECT t1.artist
	,t2.qtd_artist
	,t1.song
	,t3.qtd_song
FROM PUBLIC."Billboard1" AS t1
LEFT JOIN (
	SELECT t1.artist
		,count(*) AS qtd_artist
	FROM PUBLIC."Billboard1" AS t1
	GROUP BY t1.artist 
	ORDER BY t1.artist
	) AS t2 ON (t1.artist = t2.artist)
LEFT JOIN (
	SELECT t1.song
		,count(*) AS qtd_song
	FROM PUBLIC."Billboard1" AS t1
	GROUP BY t1.song
	ORDER BY t1.song
	) AS t3 ON (t1.song = t3.song);
	
-- Ajuste com CTE
WITH cte_artist
AS (
	SELECT t1.artist
		,count(*) AS qtd_artist
	FROM PUBLIC."Billboard1" AS t1
	GROUP BY t1.artist
	ORDER BY t1.artist
	)
	,cte_song
AS (
	SELECT t1.song
		,count(*) AS qtd_song
	FROM PUBLIC."Billboard1" AS t1
	GROUP BY t1.song
	ORDER BY t1.song
	)
SELECT t1.artist
	,t2.qtd_artist
	,t1.song
	,t3.qtd_song
FROM PUBLIC."Billboard1" AS t1
LEFT JOIN cte_artist AS t2 ON (t1.artist = t2.artist)
LEFT JOIN cte_song AS t3 ON (t1.song = t3.song);

-- Ajuste Window Function
WITH CTE_BILLBOARD
AS (
	SELECT DISTINCT t1.artist
		,t1.song
	FROM PUBLIC."Billboard1" AS t1
	ORDER BY t1.artist
		,t1.song
	)
SELECT *
	,row_number() OVER (
		ORDER BY artist
			,song
		) AS "row_number"
	,row_number() OVER (
		PARTITION BY artist ORDER BY artist
			,song
		) AS "row_number_by_artist"
	,rank() OVER (
		PARTITION BY artist ORDER BY artist
			,song
		) AS "rank_artist"
	,lag(song, 1) OVER (
		ORDER BY artist
			,song
		) AS "lag_song"
	,lead(song, 1) OVER (
		ORDER BY artist
			,song
		) AS "lead_song"
	,first_value(song) OVER (
		PARTITION BY artist ORDER BY artist
			,song
		) AS "first_song"
	,last_value(song) OVER (
		PARTITION BY artist ORDER BY artist
			,song RANGE BETWEEN UNBOUNDED PRECEDING
				AND UNBOUNDED FOLLOWING
		) AS "last_song"
	,nth_value(song, 2) OVER (
		PARTITION BY artist ORDER BY artist
			,song
		) AS "nth_song"
FROM CTE_BILLBOARD

-- Primeira vez que as músicas entraram no ranking
WITH cte_dedup
AS (
	SELECT t1."date"
		,t1."rank"
		,t1.song
		,t1.artist
		,row_number() OVER (
			PARTITION BY t1.artist
			,t1.song ORDER BY t1.artist
				,t1.song
				,t1."date"
			) AS dedup_song
		,row_number() OVER (
			PARTITION BY t1.artist ORDER BY t1.artist
				,t1."date"
			) AS dedup_artist
	FROM PUBLIC."Billboard1" AS t1
	ORDER BY t1.artist
		,t1."date"
	)
SELECT t1."date"
	,t1."rank"
	,t1.artist
	,t1.song
FROM cte_dedup AS t1
WHERE t1.artist LIKE '%'
	AND t1.dedup_song = 1
	
-- Export para uma tabela
CREATE TABLE tb_first_song AS (
	WITH cte_dedup AS (
		SELECT t1."date"
			,t1."rank"
			,t1.song
			,t1.artist
			,row_number() OVER (
				PARTITION BY t1.artist
				,t1.song ORDER BY t1.artist
					,t1.song
					,t1."date"
				) AS dedup_song
			,row_number() OVER (
				PARTITION BY t1.artist ORDER BY t1.artist
					,t1."date"
				) AS dedup_artist
		FROM PUBLIC."Billboard1" AS t1
		ORDER BY t1.artist
			,t1."date"
		) SELECT t1."date"
	,t1."rank"
	,t1.artist
	,t1.song FROM cte_dedup AS t1 WHERE t1.artist LIKE '%Linkin Park'
	AND t1.dedup_song = 1
	);

-- Export para uma view
CREATE VIEW vw_song
AS
(
		SELECT *
		FROM tb_first_song
		);

INSERT INTO tb_first_song (
	WITH cte_dedup AS (
		SELECT t1."date"
			,t1."rank"
			,t1.song
			,t1.artist
			,row_number() OVER (
				PARTITION BY t1.artist
				,t1.song ORDER BY t1.artist
					,t1.song
					,t1."date"
				) AS dedup_song
			,row_number() OVER (
				PARTITION BY t1.artist ORDER BY t1.artist
					,t1."date"
				) AS dedup_artist
		FROM PUBLIC."Billboard1" AS t1
		ORDER BY t1.artist
			,t1."date"
		) SELECT t1."date"
	,t1."rank"
	,t1.artist
	,t1.song FROM cte_dedup AS t1 WHERE t1.artist LIKE '%Linkin Park%'
	AND t1.dedup_song = 1
	)
SELECT *
FROM vw_song;

CREATE
	OR replace VIEW vw_song AS (SELECT * FROM tb_first_song AS t1 WHERE t1.artist LIKE '%Linkin Park');