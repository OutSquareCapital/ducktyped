# DuckTyped

DuckTyped est une bibliothèque Python qui fournit une API fluide et intuitive pour travailler avec DuckDB, inspirée par la syntaxe de Polars. Cette bibliothèque permet de construire des requêtes SQL de manière programmatique avec un style expressif et chainable.

## Concepts de base

DuckTyped est construit autour de quelques concepts clés :

- **Table** : Représente une table DuckDB, liée à un fichier (Parquet, CSV, etc.)
- **Query** : Représente une requête SQL en construction
- **Expr** : Expressions qui peuvent être utilisées dans les requêtes (colonnes, opérations, etc.)
- **DuckType** : Types de données utilisés dans DuckDB

## Syntaxe de base

### Définir une table

```python
import ducktyped as dk
from pathlib import Path

# Créer une référence à une table
prices = dk.Table(
    filepath="prices.parquet",
    schema={
        "date": dk.Float(),
        "ticker": dk.Varchar(),
        "close": dk.Float(),
    },
)
```

### Construire une requête

```python
# Sélectionner des colonnes
query = prices.select(
    dk.col("date"),
    dk.col("ticker"),
    dk.col("close")
)

# Filtrer avec une condition
query = prices.select(
    dk.col("date"),
    dk.col("ticker"),
    dk.col("close")
).where(
    dk.col("ticker").eq("SPY")
)

# Exécuter la requête
result = query.execute()
```

### Expressions et opérations

```python
# Opérations arithmétiques
query = prices.select(
    dk.col("date"),
    dk.col("close"),
    dk.col("close").add(1).alias("close_plus_one"),
    dk.col("close").sub(dk.col("open")).div(dk.col("open")).mul(100).alias("pct_change")
)

# Fonctions mathématiques
query = prices.select(
    dk.col("close"),
    dk.col("close").sqrt().alias("sqrt_close"),
    dk.col("returns").abs().alias("abs_returns"),
    dk.col("volatility").clip(0, 1).alias("clipped_vol")
)

# Expressions conditionnelles
query = prices.select(
    dk.col("ticker"),
    dk.col("close")
).where(
    dk.col("close").gt(100).add(dk.col("volume").gt(1000000))
)
```

### Conversion de types (cast)

```python
query = prices.select(
    dk.col("date").cast(dk.Date()),
    dk.col("ticker").cast(dk.Enum(values=["SPY", "AAPL", "GOOG"])),
    dk.col("close").cast(dk.Float()).alias("close_float")
)

# Méthodes d'aide pour le cast
query = prices.select(
    dk.col("close").as_int(),
    dk.col("date").as_date(),
    dk.col("status").as_boolean()
)
```

### Fenêtres glissantes (rolling windows)

```python
query = prices.select(
    dk.col("date"),
    dk.col("close"),
    dk.col("close").rolling_mean(window=20).over(order_by=dk.col("date")).alias("ma20"),
    dk.col("close").rolling_stddev(window=20).over(order_by=dk.col("date")).alias("std20")
)
```

## Comparaison avec Polars

### Similitudes

- **API fluide** : DuckTyped adopte l'approche de chaînage des méthodes comme Polars
- **Expressions** : Utilise un système d'expressions similaire pour les opérations et transformations
- **Lazy evaluation** : Les requêtes sont construites d'abord puis exécutées seulement quand nécessaire

```python
# Polars
import polars as pl
df = pl.scan_parquet("prices.parquet").select(
    pl.col("date"),
    pl.col("ticker"),
    pl.col("close")
).filter(
    pl.col("ticker") == "SPY"
).collect()

# DuckTyped
import ducktyped as dk
prices = dk.Table("prices.parquet", schema={...})
df = prices.select(
    dk.col("date"),
    dk.col("ticker"),
    dk.col("close")
).where(
    dk.col("ticker").eq("SPY")
).execute()
```

### Différences

- **Moteur sous-jacent** : DuckTyped utilise DuckDB alors que Polars a son propre moteur
- **Définition de schéma** : DuckTyped nécessite une définition explicite du schéma
- **Noms des méthodes** : Certaines méthodes ont des noms différents :
  - `filter()` dans Polars vs `where()` dans DuckTyped
  - `collect()` dans Polars vs `execute()` dans DuckTyped
- **Opérations de comparaison** : Polars utilise des opérateurs Python (`==`, `>`, etc.) tandis que DuckTyped utilise des méthodes (`.eq()`, `.gt()`, etc.)

## Exemples complets

### Exemple 1 : Analyse de prix d'actions

```python
import ducktyped as dk
import polars as pl
from pathlib import Path

# Définir une table
prices = dk.Table(
    filepath="prices.parquet",
    schema={
        "date": dk.Float(),
        "ticker": dk.Varchar(),
        "close": dk.Float(),
        "volume": dk.Float(),
    },
)

# Construire une requête complexe
query = prices.select(
    dk.col("date").cast(dk.Date()).alias("date"),
    dk.col("ticker"),
    dk.col("close"),
    dk.col("close").rolling_mean(window=20).over(order_by=dk.col("date")).alias("ma20"),
    dk.col("close").rolling_mean(window=50).over(order_by=dk.col("date")).alias("ma50"),
    dk.col("close").div(dk.col("close").rolling_mean(window=200).over(order_by=dk.col("date"))).sub(1).mul(100).alias("pct_above_ma200")
).where(
    dk.col("ticker").eq("SPY")
)

# Afficher la requête SQL générée
print(query.explain())

# Exécuter la requête et obtenir un DataFrame Polars
result = query.execute()
print(result.head())
```

### Exemple 2 : Transformations et formattage

```python
import ducktyped as dk

# Créer une référence à une table
sales = dk.Table(
    filepath="sales.parquet",
    schema={
        "date": dk.Date(),
        "product_id": dk.Integer(),
        "category": dk.Varchar(),
        "quantity": dk.Integer(),
        "price": dk.Float(),
    },
)

# Requête avec transformations multiples
query = sales.select(
    dk.col("date"),
    dk.col("category").cast(dk.Enum(values=["Electronics", "Clothing", "Food"])).alias("category"),
    dk.col("quantity"),
    dk.col("price"),
    dk.col("quantity").mul(dk.col("price")).alias("total_sales"),
    dk.col("quantity").mul(dk.col("price")).rolling_sum(window=7).over(order_by=dk.col("date")).alias("rolling_7day_sales")
).where(
    dk.col("price").gt(10).add(dk.col("quantity").gt(5))
)

# Exécuter et afficher les résultats
result = query.execute()
print(result)
```

### Exemple 3 : Utilisation de toutes les colonnes

```python
import ducktyped as dk

# Créer une référence à une table
customers = dk.Table(
    filepath="customers.parquet",
    schema={
        "id": dk.Integer(),
        "name": dk.Varchar(),
        "email": dk.Varchar(),
        "age": dk.Integer(),
        "status": dk.Varchar(),
    },
)

# Sélectionner toutes les colonnes
query = customers.select(
    dk.all()
).where(
    dk.col("age").gte(18)
)

# Sélectionner toutes les colonnes plus des colonnes dérivées
query = customers.select(
    dk.all(),
    dk.col("age").div(100).alias("age_century")
).where(
    dk.col("status").eq("active")
)

# Exécuter la requête
result = query.execute()
print(result)
```

## Fonctions disponibles

### Opérations arithmétiques
- `.add()` - Addition
- `.sub()` - Soustraction
- `.mul()` - Multiplication
- `.div()` - Division

### Comparaisons
- `.eq()` - Égal à
- `.neq()` - Différent de
- `.gt()` - Supérieur à
- `.lt()` - Inférieur à
- `.gte()` - Supérieur ou égal à
- `.lte()` - Inférieur ou égal à

### Fonctions mathématiques
- `.sqrt()` - Racine carrée
- `.abs()` - Valeur absolue
- `.sign()` - Signe (-1, 0, 1)
- `.clip(min_val, max_val)` - Limite les valeurs entre min et max

### Fenêtres glissantes
- `.rolling_mean()` - Moyenne mobile
- `.rolling_sum()` - Somme mobile
- `.rolling_median()` - Médiane mobile
- `.rolling_min()` - Minimum mobile
- `.rolling_max()` - Maximum mobile
- `.rolling_stddev()` - Écart-type mobile
- `.rolling_kurtosis()` - Kurtosis mobile
- `.rolling_skew()` - Asymétrie mobile

### Types de données
- `dk.Integer()` - Entier
- `dk.Float()` - Nombre à virgule flottante
- `dk.Varchar(length=None)` - Chaîne de caractères
- `dk.Date()` - Date
- `dk.Boolean()` - Booléen
- `dk.Enum(values=[...])` - Énumération

## Conclusion

DuckTyped combine la puissance de DuckDB avec une API inspirée de Polars, offrant une expérience de développement fluide et intuitive. Que vous soyez familier avec Polars ou que vous cherchiez simplement un moyen élégant d'interagir avec DuckDB en Python, DuckTyped offre une solution expressive et flexible.