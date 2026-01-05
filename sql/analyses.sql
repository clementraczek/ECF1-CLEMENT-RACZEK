-- ==========================================================
-- ECF 1 : ANALYSES ANALYTIQUES - Plateforme Data Engineering
-- Auteur : Clément Raczek
-- ==========================================================

-- 1. Requête d'agrégation simple
-- Objectif : Calculer le prix moyen, minimum et maximum des livres par note (rating).
-- Utilité : Identifier si les livres les mieux notés sont statistiquement plus chers.

SELECT 
    rating AS etoiles,
    COUNT(*) AS nombre_de_livres,
    ROUND(AVG(price_gbp)::numeric, 2) AS prix_moyen_gbp,
    MIN(price_gbp) AS prix_min,
    MAX(price_gbp) AS prix_max
FROM fact_books
GROUP BY rating
ORDER BY etoiles DESC;


-- 2. Requête avec jointure (Self-Join)
-- Objectif : Identifier les produits e-commerce de la même catégorie qui ont le même prix.
-- Utilité : Détecter des opportunités de bundle ou analyser la concurrence tarifaire interne.

SELECT 
    a.category,
    a.title AS produit_A,
    b.title AS produit_B,
    a.price
FROM fact_products a
JOIN fact_products b ON a.price = b.price 
    AND a.category = b.category 
    AND a.sku < b.sku
LIMIT 10;


-- 3. Requête avec fonction de fenêtrage (Window Function)
-- Objectif : Calculer le rang de chaque produit par prix à l'intérieur de sa catégorie.
-- Utilité : Voir où se situe un produit par rapport à ses concurrents directs de même type.

SELECT 
    category,
    subcategory,
    title,
    price,
    RANK() OVER (PARTITION BY category ORDER BY price DESC) as rang_prix_categorie
FROM fact_products
WHERE price IS NOT NULL;


-- 4. Requête de classement (Top N)
-- Objectif : Lister les 5 auteurs les plus prolifiques en termes de citations.
-- Utilité : Identifier les influenceurs ou auteurs majeurs dans la base de connaissances.

SELECT 
    author,
    COUNT(*) as total_citations,
    STRING_AGG(DISTINCT tags, ' | ') as thématiques_clés
FROM fact_quotes
GROUP BY author
ORDER BY total_citations DESC
LIMIT 5;


-- 5. Requête croisant au moins 2 sources de données (Cross Source)
-- Objectif : Trouver des thématiques communes entre les livres et les citations.
-- Utilité : Corréler les produits physiques (livres) avec le contenu intellectuel (citations).
-- Note : On utilise un LIKE pour chercher des mots-clés communs (ex: 'Life', 'Love', 'Music').

SELECT 
    q.author AS auteur_citation,
    q.text AS citation,
    b.title AS livre_recommande,
    b.price_gbp AS prix_livre
FROM fact_quotes q
JOIN fact_books b ON b.title ILIKE '%' || q.tags || '%'
WHERE q.tags <> ''
LIMIT 10;