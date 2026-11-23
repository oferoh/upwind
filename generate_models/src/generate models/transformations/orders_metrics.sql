CREATE OR REFRESH MATERIALIZED VIEW gold.orders_metrics 
as 
select  o.order_id , 
    count(distinct i.item_id) items,
    count(distinct i.product_id) products,
    sum(quantity) quantity, sum(unit_price)  unit_price 
        from silver.stg_orders o
inner join 
 silver.stg_order_items i on o.order_id = i.order_id
 group by 1

