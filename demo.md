# 🐕🐱 Nutrition Monorepo - In-Depth Guide

## 📋 Overview

The **Nutrition Monorepo** is a comprehensive veterinary nutrition recommendation system that helps veterinarians and pet owners find appropriate foods for pets with medical conditions. This guide provides an in-depth look at the system's workflow and includes practical examples you can run immediately.

## 🗂️ System Architecture

```
Nutrition Monorepo
├── packages/
│   ├── extract/              # Product extraction package
│   │   ├── src/extract/      # Source code
│   │   │   ├── hills/        # Hill's specific extraction
│   │   │   ├── entrypoints.py # CLI commands
│   │   └── tests/            # Tests
│   │
│   └── chat-agent/          # Chat agent package
│       ├── src/chat_agent/   # Source code
│       │   ├── agent.py      # Main agent (LLM-based)
│       │   ├── hills.db      # Database (SQLite)
│       │   └── web_api.py    # FastAPI endpoints
│       └── tests/            # Tests
│
├── data/                    # Data files
│   ├── external/            # External PDFs
│   └── outputs/             # Extracted data
│
├── configs/                 # Configuration files
├── notebooks/               # Jupyter notebooks
├── docs/                    # Documentation
└── scripts/                 # Helper scripts
```

## 🚀 Quick Start (No LLM Required)

### 1. Environment Setup

```bash
# Activate virtual environment
source .venv/bin/activate

# Verify Python path
python -c "import sys; print('Python path:', sys.executable)"
```

### 2. Database Exploration

```bash
# Connect to database and explore
sqlite3 packages/chat-agent/src/chat_agent/hills.db "SELECT COUNT(*) as products FROM Products;"
```

### 3. Run Simple Demo

```bash
# Run the simple demo without LLM
python simple_demo.py
```

## 🔄 Core Workflow (Without LLM)

The system uses a **multi-stage workflow** to provide recommendations:

```
User Query → Knowledge Base → SQL Queries → Product Matching → Recommendations
```

### Stage 1: Knowledge Base Lookup

The knowledge base contains rules for 26 medical conditions:

```python
# Example: Chronic Kidney Disease (Dog)
ckd_rules = {
    "nutrient_rules": {
        "Protein": {"max": 40, "min": 30},
        "Phosphorus": {"max": 0.9, "min": 0.5},
        "Fat": {"max": 60, "min": 25}
    },
    "indication_mappings": ["Renal", "Kidney"],
    "key_nutritional_factors": "Phosphorus, protein, sodium"
}
```

### Stage 2: SQL Query Generation

Based on the knowledge base rules, SQL queries are generated:

```sql
-- Find products matching CKD requirements
SELECT p.product_id, p.product_name, p.company
FROM Products p
JOIN Product_Nutrients pn ON p.product_id = pn.product_id
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE n.nutrient_name = 'Phosphorus' AND pn.value <= 0.9
AND p.for_dogs = 1
```

### Stage 3: Product Matching

The system finds products that match the nutrient requirements:

```python
# Example products for CKD
ckd_products = [
    {"name": "Hill's k/d", "company": "Hill's", "phosphorus": 0.6},
    {"name": "Royal Canin Renal", "company": "Royal Canin", "phosphorus": 0.7},
]
```

### Stage 4: Recommendation Generation

Final recommendations are generated with rationale:

```python
recommendation = {
    "products": ckd_products,
    "rationale": "These products have low phosphorus levels (≤ 0.9 g/1000 kcal) which is crucial for managing chronic kidney disease in dogs."
}
```

## 📊 Database Structure

### Tables

```
1. Products                # Central product information
2. Ingredients             # Master list of ingredients
3. Product_Ingredients     # Links products to ingredients
4. Nutrients               # Nutrient definitions
5. Product_Nutrients       # Links products to nutrient values
6. Indications             # Health conditions
7. Product_Indications     # Links products to indications
8. Contraindications       # Conditions products should avoid
9. Product_Contraindications # Links products to contraindications
10. Product_Trial_Info     # Additional product information
```

### Key Statistics

```bash
# Get database statistics
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<EOF
SELECT 'Products' as table_name, COUNT(*) as count FROM Products
UNION ALL SELECT 'Ingredients', COUNT(*) FROM Ingredients
UNION ALL SELECT 'Nutrients', COUNT(*) FROM Nutrients
UNION ALL SELECT 'Indications', COUNT(*) FROM Indications
UNION ALL SELECT 'Product_Nutrients', COUNT(*) FROM Product_Nutrients;
EOF
```

## 🔍 On-the-Fly Demo Scripts

### Script 1: Basic Database Query

```bash
# Find all products for diabetes
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, p.company, i.indication_name
FROM Products p
JOIN Product_Indications pi ON p.product_id = pi.product_id
JOIN Indications i ON pi.indication_id = i.indication_id
WHERE i.indication_name LIKE '%Diabetes%'
LIMIT 10;
EOF
```

### Script 2: Nutrient Analysis

```bash
# Analyze protein content across products
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT 
    n.nutrient_name,
    MIN(pn.value) as min_value,
    MAX(pn.value) as max_value,
    AVG(pn.value) as avg_value,
    COUNT(*) as product_count
FROM Product_Nutrients pn
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE n.nutrient_name IN ('Protein', 'Fat', 'Carbohydrate / NFE')
GROUP BY n.nutrient_name;
EOF
```

### Script 3: Species Distribution

```bash
# Count products by species
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT 
    'Dogs' as species,
    SUM(for_dogs) as count
UNION ALL 
SELECT 
    'Cats' as species,
    SUM(for_cats) as count
FROM Products;
EOF
```

### Script 4: Find Products by Condition

```bash
# Find products for kidney disease
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, p.company, i.indication_name
FROM Products p
JOIN Product_Indications pi ON p.product_id = pi.product_id
JOIN Indications i ON pi.indication_id = i.indication_id
WHERE i.indication_name LIKE '%Kidney%'
   OR i.indication_name LIKE '%Renal%'
LIMIT 10;
EOF
```

### Script 5: Nutrient Range Query

```bash
# Find high-protein, low-fat products
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, p.company
FROM Products p
WHERE p.product_id IN (
    SELECT pn1.product_id
    FROM Product_Nutrients pn1
    JOIN Nutrients n1 ON pn1.nutrient_id = n1.nutrient_id
    WHERE n1.nutrient_name = 'Protein' AND pn1.value >= 80
)
AND p.product_id IN (
    SELECT pn2.product_id
    FROM Product_Nutrients pn2
    JOIN Nutrients n2 ON pn2.nutrient_id = n2.nutrient_id
    WHERE n2.nutrient_name = 'Fat' AND pn2.value <= 30
)
LIMIT 5;
EOF
```

## 📚 Knowledge Base Examples

### Dog Conditions

```python
# Available dog conditions
dog_conditions = [
    "Critical care",
    "Hyperlipidemia", 
    "Hypercholesterolemia",
    "Diabetes",
    "Food allergy",
    "Atopy",
    "Osteoarthritis",
    "Chronic CKD IRIS 1",
    "Chronic CKD IRIS 2-4",
    "Struvite dissolution",
    "Idiopathic Epilepsy",
    "Chronic pancreatitis",
    "Chronic Gastritis",
    "Obesity"
]
```

### Cat Conditions

```python
# Available cat conditions
cat_conditions = [
    "Critical care",
    "Hyperlipidemia", 
    "Hypercholesterolemia",
    "Diabetes",
    "Food allergy",
    "Atopy",
    "Chronic CKD IRIS 1",
    "Chronic CKD IRIS 2-3",
    "Chronic CKD IRIS 4",
    "Struvite",
    "Calcium mono and di oxalate",
    "Chronic pancreatitis",
    "Adult Obesity"
]
```

## 🎯 Practical Use Cases

### Use Case 1: Find Foods for Diabetic Dog

```bash
# Step 1: Find products indicated for diabetes
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, p.company, i.indication_name
FROM Products p
JOIN Product_Indications pi ON p.product_id = pi.product_id
JOIN Indications i ON pi.indication_id = i.indication_id
WHERE i.indication_name LIKE '%Diabetes%'
AND p.for_dogs = 1
LIMIT 5;
EOF

# Step 2: Check nutrient profiles of those products
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, n.nutrient_name, pn.value, n.unit
FROM Products p
JOIN Product_Nutrients pn ON p.product_id = pn.product_id
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE p.product_name LIKE '%Chicken%'
AND n.nutrient_name IN ('Protein', 'Fat', 'Carbohydrate / NFE')
LIMIT 10;
EOF
```

### Use Case 2: Find Low-Phosphorus Foods for CKD

```bash
# Find products with low phosphorus for kidney disease
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, p.company, pn.value
FROM Products p
JOIN Product_Nutrients pn ON p.product_id = pn.product_id
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE n.nutrient_name = 'Phosphorus'
AND pn.value <= 0.9
ORDER BY pn.value ASC
LIMIT 5;
EOF
```

### Use Case 3: Compare Nutrient Profiles

```bash
# Compare protein content between two products
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT 
    p.product_name,
    n.nutrient_name,
    pn.value,
    n.unit
FROM Products p
JOIN Product_Nutrients pn ON p.product_id = pn.product_id
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE p.product_name IN ('with Chicken (Dry)', 'with Lamb (Dry)')
AND n.nutrient_name IN ('Protein', 'Fat', 'Carbohydrate / NFE', 'Phosphorus')
ORDER BY p.product_name, n.nutrient_name;
EOF
```

## 🛠️ Advanced Queries

### Query 1: Find Products with Multiple Indications

```bash
# Find products good for both diabetes and weight management
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, p.company, COUNT(DISTINCT i.indication_name) as indication_count
FROM Products p
JOIN Product_Indications pi ON p.product_id = pi.product_id
JOIN Indications i ON pi.indication_id = i.indication_id
WHERE (i.indication_name LIKE '%Diabetes%' 
      OR i.indication_name LIKE '%Weight%')
AND p.for_dogs = 1
GROUP BY p.product_id
HAVING COUNT(DISTINCT i.indication_name) > 1
LIMIT 5;
EOF
```

### Query 2: Nutrient Correlation Analysis

```bash
# Find products with balanced protein-to-fat ratio
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT 
    p.product_name,
    p.company,
    protein.value as protein,
    fat.value as fat,
    printf("%.2f", protein.value / fat.value) as protein_fat_ratio
FROM Products p
JOIN Product_Nutrients protein ON p.product_id = protein.product_id
JOIN Nutrients n1 ON protein.nutrient_id = n1.nutrient_id
JOIN Product_Nutrients fat ON p.product_id = fat.product_id
JOIN Nutrients n2 ON fat.nutrient_id = n2.nutrient_id
WHERE n1.nutrient_name = 'Protein' 
AND n2.nutrient_name = 'Fat'
AND protein.value / fat.value BETWEEN 1.5 AND 3.0
LIMIT 10;
EOF
```

### Query 3: Ingredient Analysis

```bash
# Find products containing specific ingredients
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT p.product_name, p.company, i.ingredient_name
FROM Products p
JOIN Product_Ingredients pi ON p.product_id = pi.product_id
JOIN Ingredients i ON pi.ingredient_id = i.ingredient_id
WHERE i.ingredient_name LIKE '%Chicken%'
ORDER BY pi.ingredient_order
LIMIT 10;
EOF
```

## 📈 Data Analysis Examples

### Example 1: Nutrient Distribution

```bash
# Analyze distribution of key nutrients
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT 
    n.nutrient_name,
    MIN(pn.value) as min,
    MAX(pn.value) as max,
    AVG(pn.value) as avg,
    COUNT(*) as count
FROM Product_Nutrients pn
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE n.nutrient_name IN ('Protein', 'Fat', 'Carbohydrate / NFE', 'Phosphorus', 'Calcium')
GROUP BY n.nutrient_name
ORDER BY avg DESC;
EOF
```

### Example 2: Species Comparison

```bash
# Compare nutrient profiles between dog and cat foods
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT 
    CASE WHEN p.for_dogs = 1 THEN 'Dog' ELSE 'Cat' END as species,
    n.nutrient_name,
    AVG(pn.value) as avg_value,
    COUNT(*) as product_count
FROM Products p
JOIN Product_Nutrients pn ON p.product_id = pn.product_id
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE n.nutrient_name = 'Protein'
GROUP BY species, n.nutrient_name;
EOF
```

### Example 3: Indication Frequency

```bash
# Find most common indications
sqlite3 packages/chat-agent/src/chat_agent/hills.db <<'EOF'
SELECT 
    i.indication_name,
    COUNT(*) as product_count
FROM Product_Indications pi
JOIN Indications i ON pi.indication_id = i.indication_id
GROUP BY i.indication_name
ORDER BY product_count DESC
LIMIT 10;
EOF
```

## 🎓 Workflow Summary (Without LLM)

### Step-by-Step Process

1. **Input**: User specifies medical condition (e.g., "kidney disease")
2. **Lookup**: System finds matching rules in knowledge base
3. **Query Generation**: SQL queries created based on nutrient rules
4. **Database Search**: Products matching nutrient criteria are found
5. **Filtering**: Additional filters applied (species, indications, etc.)
6. **Output**: List of recommended products with rationale

### Example Workflow

```python
# 1. User Input
user_query = "kidney disease in dogs"

# 2. Knowledge Base Lookup
condition_rules = {
    "nutrient_rules": {
        "Protein": {"max": 40, "min": 30},
        "Phosphorus": {"max": 0.9, "min": 0.5}
    }
}

# 3. SQL Query Generation
sql_query = """
SELECT p.product_id, p.product_name
FROM Products p
JOIN Product_Nutrients pn1 ON p.product_id = pn1.product_id
JOIN Nutrients n1 ON pn1.nutrient_id = n1.nutrient_id
JOIN Product_Nutrients pn2 ON p.product_id = pn2.product_id
JOIN Nutrients n2 ON pn2.nutrient_id = n2.nutrient_id
WHERE n1.nutrient_name = 'Protein' AND pn1.value BETWEEN 30 AND 40
AND n2.nutrient_name = 'Phosphorus' AND pn2.value <= 0.9
AND p.for_dogs = 1
"""

# 4. Database Search
# (Execute SQL query)

# 5. Results
recommended_products = [
    {"name": "Hill's k/d", "phosphorus": 0.6, "protein": 35},
    {"name": "Royal Canin Renal", "phosphorus": 0.7, "protein": 38}
]

# 6. Output
print("Recommended products for kidney disease:")
for product in recommended_products:
    print(f"  • {product['name']}: P={product['protein']}g, Phos={product['phosphorus']}g")
```

## 🔧 Troubleshooting

### Common Issues and Solutions

**Issue 1: Database not found**
```bash
# Solution: Check database path
ls packages/chat-agent/src/chat_agent/hills.db
```

**Issue 2: Module not found**
```bash
# Solution: Activate virtual environment
source .venv/bin/activate
```

**Issue 3: SQL syntax error**
```bash
# Solution: Test query in SQLite directly
sqlite3 packages/chat-agent/src/chat_agent/hills.db "YOUR_QUERY"
```

**Issue 4: No products found**
```bash
# Solution: Check query conditions
# Try broader search criteria
```

## 📋 Quick Reference

### Common SQL Patterns

```sql
-- Find products for specific condition
SELECT p.* FROM Products p
JOIN Product_Indications pi ON p.product_id = pi.product_id
JOIN Indications i ON pi.indication_id = i.indication_id
WHERE i.indication_name LIKE '%CONDITION%'

-- Find products with nutrient criteria
SELECT p.* FROM Products p
JOIN Product_Nutrients pn ON p.product_id = pn.product_id
JOIN Nutrients n ON pn.nutrient_id = n.nutrient_id
WHERE n.nutrient_name = 'NUTRIENT' AND pn.value OPERATOR VALUE

-- Find products for specific species
SELECT p.* FROM Products p
WHERE p.for_dogs = 1 OR p.for_cats = 1
```

### Useful Commands

```bash
# Count products
sqlite3 hills.db "SELECT COUNT(*) FROM Products;"

# List tables
sqlite3 hills.db ".tables"

# Show table schema
sqlite3 hills.db "PRAGMA table_info(Products);"

# Export query results
sqlite3 hills.db "SELECT * FROM Products LIMIT 5;" > output.csv
```

## 🎯 Conclusion

This in-depth guide provides everything you need to:

1. **Understand the system architecture** and workflow
2. **Run on-the-fly demo scripts** without LLM requirements
3. **Explore the database** with practical SQL queries
4. **Analyze nutrient profiles** and product recommendations
5. **Troubleshoot common issues** with solutions

The system is **fully functional without LLM** and provides comprehensive veterinary nutrition recommendations based on:
- **135 veterinary products** with detailed nutrient information
- **26 medical conditions** with treatment guidelines
- **SQL-based product matching** for intelligent recommendations
- **Comprehensive database** for analysis and exploration

**Next Steps:**
- Run the demo scripts to explore functionality
- Try the SQL queries to understand the data
- Extend with additional conditions as needed
- Integrate with your workflow for production use

**The Nutrition Monorepo is ready for use!** 🎉🐕🐱