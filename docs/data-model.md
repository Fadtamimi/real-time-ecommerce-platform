# Phase 1 Data Model

## Entities

### customers

| Column | Type | Rules |
| --- | --- | --- |
| `customer_id` | string | Primary key, required |
| `name` | string | Required |
| `country` | string | Required |
| `signup_date` | date | Required, ISO-8601 format |

### products

| Column | Type | Rules |
| --- | --- | --- |
| `product_id` | string | Primary key, required |
| `product_name` | string | Required |
| `category` | string | Required |
| `price` | decimal | Required, greater than zero |

### events

| Column | Type | Rules |
| --- | --- | --- |
| `event_id` | string | Primary key, required |
| `customer_id` | string | Required foreign key to `customers.customer_id` |
| `product_id` | string | Required foreign key to `products.product_id` |
| `event_type` | string | Required; `view`, `add_to_cart`, or `purchase` |
| `timestamp` | timestamp | Required, UTC ISO-8601 format |
| `quantity` | integer | Required, greater than zero |

## Relationships

- One customer can produce many events.
- One product can appear in many events.
- Customers and products have a many-to-many relationship through events.

The model is normalized because customer and product attributes are stored once,
while events reference them through foreign keys instead of repeating groups such
as `product_1`, `product_2`, and `product_3`.

## Initial quality rules

- Primary keys must be unique and non-empty.
- Event foreign keys must reference an existing customer and product.
- Required fields must not be blank.
- Product prices and event quantities must be positive.
- Dates and timestamps must be parseable using standard ISO-8601 formats.
