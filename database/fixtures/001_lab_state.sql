-- État initial déterministe du laboratoire : aucune opération traitée, aucune commande préparée.
-- (Les commandes ORD-101..104 vivent dans le service Commandes, en mémoire ; PostgreSQL ne stocke que l'effet de préparation.)
-- Ne touche QUE ces deux tables.
TRUNCATE TABLE processed_operations, prepared_orders;
