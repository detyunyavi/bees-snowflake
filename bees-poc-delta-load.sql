use role EDW_DEVELOPER;
use warehouse EDW_CDF_VW2;
use EDW_SBX.STG;

select count(1) as NEW_REC from EDW_SBX.STG.S1_CDF_BEES_ORDERS where MSG_PROCID is null;

-- temp S1_CDF_BEES_ORDERS to be inserted to S2
create or replace temporary table temp_S1_CDF_BEES_ORDERS as (select distinct 
       MSG_KEY                                                                            -- comment 'Message auto incremental Key in a batch'
      ,MSG_UPDT
      ,convert_timezone('UTC',MSG_UPDT)                              as MSG_UPC           -- comment 'Message arrived on (UTC)'
      ,(MSG_KEY+(to_char(MSG_UPC,'yyyymmddhhmissff')::int)*1000000)  as MSG_INT           -- comment 'Message arrived on (int) + Mssage Key in a batch'    
      ,MSG_TYPE
      ,parse_json(MSG_CNTS):order:orderNumber::varchar               as orderNumber       -- comment 'Order Number'  
      ,parse_json(MSG_CNTS):order:placementDate::timestamp_ntz       as placementDate     -- comment 'Order Placement Date'     
      ,parse_json(MSG_CNTS):order:status::varchar                    as status            -- comment 'Order Status'
      ,parse_json(MSG_CNTS):order:previousStatus::varchar            as previousStatus    -- comment 'previous Order Status'
      ,parse_json(MSG_CNTS):order:audit:createAt::timestamp_ntz      as audit_createAt    -- comment 'Audit Created At...'
      ,parse_json(MSG_CNTS):order:audit:updateAt::timestamp_ntz      as audit_updateAt    -- comment 'Audit Updated At...'
      ,parse_json(MSG_CNTS):order:delivery:deliveryCenterId::varchar as delivery_CenterId -- comment 'Delivery Center Id'
      ,parse_json(MSG_CNTS):order:delivery:date::timestamp_ntz       as delivery_Date     -- comment 'Delivery Date'
      ,parse_json(MSG_CNTS):order:channel::varchar                   as channel
      ,parse_json(MSG_CNTS):order:beesAccountId::varchar             as beesAccountId
      ,parse_json(MSG_CNTS):order:deleted::boolean                   as deleted
      ,parse_json(MSG_CNTS):order:summary:discount::float            as ord_sum_discount
      ,parse_json(MSG_CNTS):order:summary:subtotal::float            as ord_sum_subtotal
      ,parse_json(MSG_CNTS):order:summary:total::float               as ord_sum_total
      ,parse_json(MSG_CNTS):order:vendor:id::varchar                 as vendor_id
      ,parse_json(MSG_CNTS):order:vendor:accountId::varchar          as vendor_accountId
      ,'BEES799'                                                     as SITE_ID
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)))         as KEYCOLS_HASH       -- comment 'Order Key'
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)
         ,iff(equal_null(placementDate,null),'',to_varchar(placementDate))
         ,iff(equal_null(status,null),'',status)
         ,iff(equal_null(previousStatus,null),'',previousStatus)
         ,iff(equal_null(audit_createAt,null),'',to_varchar(audit_createAt))
         ,iff(equal_null(audit_updateAt,null),'',to_varchar(audit_updateAt))
         ,iff(equal_null(delivery_CenterId,null),'',delivery_CenterId)
         ,iff(equal_null(delivery_Date,null),'',to_varchar(delivery_Date))
         ,iff(equal_null(channel,null),'',channel)
         ,iff(equal_null(beesAccountId,null),'',beesAccountId)
         ,iff(equal_null(deleted,null),'',deleted)
         ,iff(equal_null(ord_sum_discount,null),'',to_varchar(ord_sum_discount))
         ,iff(equal_null(ord_sum_subtotal,null),'',to_varchar(ord_sum_subtotal))
         ,iff(equal_null(ord_sum_total,null),'',to_varchar(ord_sum_total))
         ,iff(equal_null(vendor_id,null),'',vendor_id)
         ,iff(equal_null(vendor_accountId,null),'',vendor_accountId)
         ))                                                          as ALLCOLS_HASH       -- comment 'Message Content'
        from EDW_SBX.STG.S1_CDF_BEES_ORDERS
     where MSG_TYPE = 'Inbound' and MSG_PROCID is null and orderNumber is not null
     union all select distinct
       MSG_KEY                                                                            -- comment 'Message auto incremental Key in a batch'
      ,MSG_UPDT
      ,convert_timezone('UTC',MSG_UPDT)                              as MSG_UPC           -- comment 'Message arrived on (UTC)'
      ,(MSG_KEY+(to_char(MSG_UPC,'yyyymmddhhmissff')::int)*1000000)  as MSG_INT           -- comment 'Message arrived on (int) + Mssage Key in a batch'     
      ,MSG_TYPE
      ,parse_json(MSG_CNTS):orderNumber::varchar                     as orderNumber       -- comment 'Order Number'  
      ,parse_json(MSG_CNTS):placementDate::timestamp_ntz             as placementDate     -- comment 'Order Placement Date'     
      ,parse_json(MSG_CNTS):status::varchar                          as status            -- comment 'Order Status'
      ,parse_json(MSG_CNTS):previousStatus::varchar                  as previousStatus    -- comment 'previous Order Status'
      ,parse_json(MSG_CNTS):audit:createAt::timestamp_ntz            as audit_createAt    -- comment 'Audit Created At...'
      ,parse_json(MSG_CNTS):audit:updateAt::timestamp_ntz            as audit_updateAt    -- comment 'Audit Updated At...'
      ,parse_json(MSG_CNTS):delivery:deliveryCenterId::varchar       as delivery_CenterId -- comment 'Delivery Center Id'
      ,parse_json(MSG_CNTS):delivery:date::timestamp_ntz             as delivery_Date     -- comment 'Delivery Date'
      ,parse_json(MSG_CNTS):channel::varchar                         as channel
      ,parse_json(MSG_CNTS):beesAccountId::varchar                   as beesAccountId
      ,parse_json(MSG_CNTS):deleted::boolean                         as deleted
      ,parse_json(MSG_CNTS):summary:discount::float                  as ord_sum_discount
      ,parse_json(MSG_CNTS):summary:subtotal::float                  as ord_sum_subtotal
      ,parse_json(MSG_CNTS):summary:total::float                     as ord_sum_total
      ,parse_json(MSG_CNTS):vendor:id::varchar                       as vendor_id
      ,parse_json(MSG_CNTS):vendor:accountId::varchar                as vendor_accountId
      ,'BEES799'                                                     as SITE_ID
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)))         as KEYCOLS_HASH       -- comment 'Order Key'
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)
         ,iff(equal_null(placementDate,null),'',to_varchar(placementDate))
         ,iff(equal_null(status,null),'',status)
         ,iff(equal_null(previousStatus,null),'',previousStatus)
         ,iff(equal_null(audit_createAt,null),'',to_varchar(audit_createAt))
         ,iff(equal_null(audit_updateAt,null),'',to_varchar(audit_updateAt))
         ,iff(equal_null(delivery_CenterId,null),'',delivery_CenterId)
         ,iff(equal_null(delivery_Date,null),'',to_varchar(delivery_Date))
         ,iff(equal_null(channel,null),'',channel)
         ,iff(equal_null(beesAccountId,null),'',beesAccountId)
         ,iff(equal_null(deleted,null),'',deleted)
         ,iff(equal_null(ord_sum_discount,null),'',to_varchar(ord_sum_discount))
         ,iff(equal_null(ord_sum_subtotal,null),'',to_varchar(ord_sum_subtotal))
         ,iff(equal_null(ord_sum_total,null),'',to_varchar(ord_sum_total))
         ,iff(equal_null(vendor_id,null),'',vendor_id)
         ,iff(equal_null(vendor_accountId,null),'',vendor_accountId)
         ))                                                          as ALLCOLS_HASH       -- comment 'Message Content'
        from EDW_SBX.STG.S1_CDF_BEES_ORDERS
     where MSG_TYPE = 'Outbound' and MSG_PROCID is null and orderNumber is not null);


-- insert to S2     
insert into EDW_SBX.INT.S2_CDF_BEES_ORDERS 
    (MSG_KEY        ,MSG_UPDT         ,MSG_UPC
    ,MSG_INT        ,MSG_TYPE         ,orderNumber
    ,placementDate  ,status           ,previousStatus
    ,audit_createAt ,audit_updateAt   ,delivery_CenterId
    ,delivery_Date  ,channel          ,beesAccountId
    ,deleted        ,ord_sum_discount ,ord_sum_subtotal
    ,ord_sum_total  ,vendor_id        ,vendor_accountId
    ,SITE_ID        ,KEYCOLS_HASH     ,ALLCOLS_HASH)
    -- ,MSG_PROCID  ,MSG_PROCON
    (select * from temp_S1_CDF_BEES_ORDERS);


-- update S1 processed records
merge into EDW_SBX.STG.S1_CDF_BEES_ORDERS tgt
    using (select * from temp_S1_CDF_BEES_ORDERS) src
    on tgt.MSG_KEY=src.MSG_KEY and tgt.MSG_UPDT=src.MSG_UPDT
    when matched then update set
        tgt.MSG_PROCID=0, tgt.MSG_PROCON=current_timestamp;  -- 0: message has been processed to S2 Order Headers


-- temp S2_CDF_BEES_ORDERS to be inserted to S3 for MSG_TYPE='Outbound'
create or replace temporary table temp_S2_CDF_BEES_ORDERS as
select distinct
     MSG_KEY        ,MSG_UPDT         ,MSG_UPC
    ,MSG_INT        ,MSG_TYPE         ,orderNumber
    ,rank() over (partition by orderNumber order by MSG_INT desc) as LAST_UPDATE
    ,placementDate  ,status           ,previousStatus
    ,audit_createAt ,audit_updateAt   ,delivery_CenterId
    ,delivery_Date  ,channel          ,beesAccountId
    ,deleted        ,ord_sum_discount ,ord_sum_subtotal
    ,ord_sum_total  ,vendor_id        ,vendor_accountId
    ,SITE_ID        ,KEYCOLS_HASH     ,ALLCOLS_HASH
    from EDW_SBX.INT.S2_CDF_BEES_ORDERS
        where MSG_TYPE='Outbound' and MSG_PROCID is null;


-- merge into S3 for MSG_TYPE='Outbound'
merge into EDW_SBX.INT.S3_CDF_BEES_ORDERS tgt
    using (select * from temp_S2_CDF_BEES_ORDERS where LAST_UPDATE=1) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH
    when matched and tgt.ALLCOLS_HASH!= src.ALLCOLS_HASH then update set
         tgt.MSG_KEY=ifnull(src.MSG_KEY,tgt.MSG_KEY)                      ,tgt.MSG_UPDT=ifnull(src.MSG_UPDT,tgt.MSG_UPDT)                         ,tgt.MSG_UPC=ifnull(src.MSG_UPC,tgt.MSG_UPC)
        ,tgt.MSG_INT=ifnull(src.MSG_INT,tgt.MSG_INT)                      ,tgt.MSG_TYPE=ifnull(src.MSG_TYPE,tgt.MSG_TYPE)                         ,tgt.orderNumber=ifnull(src.orderNumber,tgt.orderNumber)
        ,tgt.placementDate=ifnull(src.placementDate,tgt.placementDate)    ,tgt.status=ifnull(src.status,tgt.status)                               ,tgt.previousStatus=ifnull(src.previousStatus,tgt.previousStatus)
        ,tgt.audit_createAt=ifnull(src.audit_createAt,tgt.audit_createAt) ,tgt.audit_updateAt=ifnull(src.audit_updateAt,tgt.audit_updateAt)       ,tgt.delivery_CenterId=ifnull(src.delivery_CenterId,tgt.delivery_CenterId)
        ,tgt.delivery_Date=ifnull(src.delivery_Date,tgt.delivery_Date)    ,tgt.channel=ifnull(src.channel,tgt.channel)                            ,tgt.beesAccountId=ifnull(src.beesAccountId,tgt.beesAccountId)
        ,tgt.deleted=ifnull(src.deleted,tgt.deleted)                      ,tgt.ord_sum_discount=ifnull(src.ord_sum_discount,tgt.ord_sum_discount) ,tgt.ord_sum_subtotal=ifnull(src.ord_sum_subtotal,tgt.ord_sum_subtotal)
        ,tgt.ord_sum_total=ifnull(src.ord_sum_total,tgt.ord_sum_total)    ,tgt.vendor_id=ifnull(src.vendor_id,tgt.vendor_id)                      ,tgt.vendor_accountId=ifnull(src.vendor_accountId,tgt.vendor_accountId)
        ,tgt.SITE_ID=ifnull(src.SITE_ID,tgt.SITE_ID)                      ,tgt.KEYCOLS_HASH=ifnull(src.KEYCOLS_HASH,tgt.KEYCOLS_HASH)             ,tgt.ALLCOLS_HASH=ifnull(src.ALLCOLS_HASH,tgt.ALLCOLS_HASH)
        ,tgt.MSG_PROCID=12                                                ,tgt.MSG_PROCON=current_timestamp
   when not matched then insert (
         MSG_KEY,        MSG_UPDT,         MSG_UPC
        ,MSG_INT,        MSG_TYPE,         orderNumber
        ,placementDate,  status,           previousStatus       
        ,audit_createAt, audit_updateAt,   delivery_CenterId
        ,delivery_Date,  channel,          beesAccountId
        ,deleted,        ord_sum_discount, ord_sum_subtotal
        ,ord_sum_total,  vendor_id,        vendor_accountId
        ,SITE_ID,        KEYCOLS_HASH,     ALLCOLS_HASH
        ,MSG_PROCID,     MSG_PROCON)
       values (
         src.MSG_KEY,        src.MSG_UPDT,         src.MSG_UPC
        ,src.MSG_INT,        src.MSG_TYPE,         src.orderNumber
        ,src.placementDate,  src.status,           src.previousStatus       
        ,src.audit_createAt, src.audit_updateAt,   src.delivery_CenterId
        ,src.delivery_Date,  src.channel,          src.beesAccountId
        ,src.deleted,        src.ord_sum_discount, src.ord_sum_subtotal
        ,src.ord_sum_total,  src.vendor_id,        src.vendor_accountId
        ,src.SITE_ID,        src.KEYCOLS_HASH,     src.ALLCOLS_HASH
        ,11,                 current_timestamp);                            


-- update S2 processed records        
merge into EDW_SBX.INT.S2_CDF_BEES_ORDERS tgt
    using (select * from temp_S2_CDF_BEES_ORDERS) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH and tgt.MSG_INT=src.MSG_INT
    when matched then update set
        tgt.MSG_PROCID=10, tgt.MSG_PROCON=current_timestamp;                                -- 10 : message has been processed to S3 Order Headers as Get


-- temp S2_CDF_BEES_ORDERS to be inserted to S3 for MSG_TYPE='Inbound'
create or replace temporary table temp_S2_CDF_BEES_ORDERS as
select distinct
     MSG_KEY        ,MSG_UPDT         ,MSG_UPC
    ,MSG_INT        ,MSG_TYPE         ,orderNumber
    ,rank() over (partition by orderNumber order by MSG_INT desc) as LAST_UPDATE
    ,placementDate  ,status           ,previousStatus
    ,audit_createAt ,audit_updateAt   ,delivery_CenterId
    ,delivery_Date  ,channel          ,beesAccountId
    ,deleted        ,ord_sum_discount ,ord_sum_subtotal
    ,ord_sum_total  ,vendor_id        ,vendor_accountId
    ,SITE_ID        ,KEYCOLS_HASH     ,ALLCOLS_HASH
    from EDW_SBX.INT.S2_CDF_BEES_ORDERS
        where MSG_TYPE='Inbound' and MSG_PROCID is null;
              
          
-- merge into S3 for MSG_TYPE='Inbound'
merge into EDW_SBX.INT.S3_CDF_BEES_ORDERS tgt
    using (select * from temp_S2_CDF_BEES_ORDERS where LAST_UPDATE=1) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH
    when matched then update set
         tgt.MSG_KEY=ifnull(src.MSG_KEY,tgt.MSG_KEY)                      ,tgt.MSG_UPDT=ifnull(src.MSG_UPDT,tgt.MSG_UPDT)                         ,tgt.MSG_UPC=ifnull(src.MSG_UPC,tgt.MSG_UPC)
        ,tgt.MSG_INT=ifnull(src.MSG_INT,tgt.MSG_INT)                      ,tgt.MSG_TYPE=ifnull(src.MSG_TYPE,tgt.MSG_TYPE)                         ,tgt.orderNumber=ifnull(src.orderNumber,tgt.orderNumber)
        ,tgt.placementDate=ifnull(src.placementDate,tgt.placementDate)    ,tgt.status=ifnull(src.status,tgt.status)                               ,tgt.previousStatus=ifnull(src.previousStatus,tgt.previousStatus)
        ,tgt.audit_createAt=ifnull(src.audit_createAt,tgt.audit_createAt) ,tgt.audit_updateAt=ifnull(src.audit_updateAt,tgt.audit_updateAt)       ,tgt.delivery_CenterId=ifnull(src.delivery_CenterId,tgt.delivery_CenterId)
        ,tgt.delivery_Date=ifnull(src.delivery_Date,tgt.delivery_Date)    ,tgt.channel=ifnull(src.channel,tgt.channel)                            ,tgt.beesAccountId=ifnull(src.beesAccountId,tgt.beesAccountId)
        ,tgt.deleted=ifnull(src.deleted,tgt.deleted)                      ,tgt.ord_sum_discount=ifnull(src.ord_sum_discount,tgt.ord_sum_discount) ,tgt.ord_sum_subtotal=ifnull(src.ord_sum_subtotal,tgt.ord_sum_subtotal)
        ,tgt.ord_sum_total=ifnull(src.ord_sum_total,tgt.ord_sum_total)    ,tgt.vendor_id=ifnull(src.vendor_id,tgt.vendor_id)                      ,tgt.vendor_accountId=ifnull(src.vendor_accountId,tgt.vendor_accountId)
        ,tgt.SITE_ID=ifnull(src.SITE_ID,tgt.SITE_ID)                      ,tgt.KEYCOLS_HASH=ifnull(src.KEYCOLS_HASH,tgt.KEYCOLS_HASH)             ,tgt.ALLCOLS_HASH=ifnull(src.ALLCOLS_HASH,tgt.ALLCOLS_HASH)
        ,tgt.MSG_PROCID=22                                                ,tgt.MSG_PROCON=current_timestamp
   when not matched then insert (
         MSG_KEY,        MSG_UPDT,         MSG_UPC
        ,MSG_INT,        MSG_TYPE,         orderNumber
        ,placementDate,  status,           previousStatus       
        ,audit_createAt, audit_updateAt,   delivery_CenterId
        ,delivery_Date,  channel,          beesAccountId
        ,deleted,        ord_sum_discount, ord_sum_subtotal
        ,ord_sum_total,  vendor_id,        vendor_accountId
        ,SITE_ID,        KEYCOLS_HASH,     ALLCOLS_HASH
        ,MSG_PROCID,     MSG_PROCON)
       values (
         src.MSG_KEY,        src.MSG_UPDT,         src.MSG_UPC
        ,src.MSG_INT,        src.MSG_TYPE,         src.orderNumber
        ,src.placementDate,  src.status,           src.previousStatus       
        ,src.audit_createAt, src.audit_updateAt,   src.delivery_CenterId
        ,src.delivery_Date,  src.channel,          src.beesAccountId
        ,src.deleted,        src.ord_sum_discount, src.ord_sum_subtotal
        ,src.ord_sum_total,  src.vendor_id,        src.vendor_accountId
        ,src.SITE_ID,        src.KEYCOLS_HASH,     src.ALLCOLS_HASH
        ,21,                 current_timestamp);

        
-- update S2 processed records          
merge into EDW_SBX.INT.S2_CDF_BEES_ORDERS tgt
    using (select * from temp_S2_CDF_BEES_ORDERS) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH and tgt.MSG_INT=src.MSG_INT
    when matched then update set
        tgt.MSG_PROCID=20, tgt.MSG_PROCON=current_timestamp;                                -- 20 : message has been processed to S3 Order Headers as Patch

        
-- temp S1_CDF_BEES_ITEMS to be inserted to S2
create or replace temporary table temp_S1_CDF_BEES_ITEMS as (select distinct 
       MSG_KEY                                                                            -- comment 'Message auto incremental Key in a batch'
      ,MSG_UPDT
      ,convert_timezone('UTC',MSG_UPDT)                              as MSG_UPC           -- comment 'Message arrived on (UTC)'
      ,(MSG_KEY+(to_char(MSG_UPC,'yyyymmddhhmissff')::int)*1000000)  as MSG_INT           -- comment 'Message arrived on (int) + Mssage Key in a batch'    
      ,MSG_TYPE
      ,parse_json(MSG_CNTS):orderNumber::varchar                     as orderNumber       -- comment 'Order Number'
      ,itm.value:key::varchar                                        as itm_key
      ,itm.value:sku::int                                            as sku  
      ,itm.value:name::varchar                                       as name
      ,itm.value:image::varchar                                      as image
      ,itm.value:package:itemCount::int                              as pack_itemCount
      ,itm.value:package:packageId::varchar                          as pack_packageId
      ,itm.value:package:unitCount::int                              as pack_unitCount
      ,itm.value:package:pack::varchar                               as pack_pack
      ,itm.value:package:name::varchar                               as pack_name
      ,itm.value:package:unitOfMeasurement::varchar                  as pack_UoM
      ,itm.value:measureUnit::varchar                                as measureUnit
      ,itm.value:typeOfUnit::varchar                                 as typeOfUnit
      ,itm.value:vendorItemId::varchar                               as vendorItemId                             
      ,itm.value:type::varchar                                       as type
      ,itm.value:quantity::float                                     as quantity
      ,itm.value:itemClassification::varchar                         as itemClassification
      ,itm.value:summaryItem:discount::float                         as itm_sum_discount
      ,itm.value:summaryItem:price::float                            as itm_sum_price
      ,itm.value:summaryItem:originalPrice::float                    as itm_sum_originalPrice
      ,itm.value:summaryItem:subtotal::float                         as itm_sum_subtotal
      ,itm.value:summaryItem:total::float                            as itm_sum_total
      ,tax.value:id::varchar                                         as tax_id
      ,tax.value:value::float                                        as tax_value
      ,'BEES799'                                                     as SITE_ID
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)
         ,iff(equal_null(itm_key,null),'',itm_key)))                 as KEYCOLS_HASH       -- comment 'Order Key'
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)
         ,iff(equal_null(itm_key,null),'',itm_key)
         ,iff(equal_null(sku,null),'',to_varchar(sku))
         ,iff(equal_null(name,null),'',name)
         ,iff(equal_null(image,null),'',image)
         ,iff(equal_null(pack_itemCount,null),'',to_varchar(pack_itemCount))
         ,iff(equal_null(pack_packageId,null),'',pack_packageId)
         ,iff(equal_null(pack_unitCount,null),'',to_varchar(pack_unitCount))
         ,iff(equal_null(pack_pack,null),'',pack_pack)
         ,iff(equal_null(pack_name,null),'',pack_name)
         ,iff(equal_null(pack_UoM,null),'',pack_UoM)
         ,iff(equal_null(measureUnit,null),'',measureUnit)
         ,iff(equal_null(typeOfUnit,null),'',typeOfUnit)
         ,iff(equal_null(vendorItemId,null),'',vendorItemId)
         ,iff(equal_null(type,null),'',type)
         ,iff(equal_null(quantity,null),'',to_varchar(quantity))
         ,iff(equal_null(itemClassification,null),'',itemClassification)
         ,iff(equal_null(itm_sum_discount,null),'',to_varchar(itm_sum_discount))
         ,iff(equal_null(itm_sum_price,null),'',to_varchar(itm_sum_price))
         ,iff(equal_null(itm_sum_originalPrice,null),'',to_varchar(itm_sum_originalPrice))
         ,iff(equal_null(itm_sum_subtotal,null),'',to_varchar(itm_sum_subtotal))
         ,iff(equal_null(itm_sum_total,null),'',to_varchar(itm_sum_total))
         ,iff(equal_null(tax_id,null),'',tax_id)
         ,iff(equal_null(tax_value,null),'',to_varchar(tax_value)))) as ALLCOLS_HASH       -- comment 'Message Content'
        from EDW_SBX.STG.S1_CDF_BEES_ORDERS
            ,lateral flatten(input => parse_json(MSG_CNTS):items, outer => true)  itm
            ,lateral flatten(input => itm.value:summaryItem:taxes, outer => true) tax
     where MSG_TYPE = 'Outbound' and MSG_PROCID=0 and itm_key is not null
     union all select distinct 
       MSG_KEY                                                                            -- comment 'Message auto incremental Key in a batch'
      ,MSG_UPDT
      ,convert_timezone('UTC',MSG_UPDT)                              as MSG_UPC           -- comment 'Message arrived on (UTC)'
      ,(MSG_KEY+(to_char(MSG_UPC,'yyyymmddhhmissff')::int)*1000000)  as MSG_INT           -- comment 'Message arrived on (int) + Mssage Key in a batch'       
      ,MSG_TYPE
      ,parse_json(MSG_CNTS):order:orderNumber::varchar               as orderNumber       -- comment 'Order Number'  
      ,itm.value:key::varchar                                        as itm_key
      ,itm.value:sku::int                                            as sku  
      ,itm.value:name::varchar                                       as name
      ,itm.value:image::varchar                                      as image
      ,itm.value:package:itemCount::int                              as pack_itemCount
      ,itm.value:package:packageId::varchar                          as pack_packageId
      ,itm.value:package:unitCount::int                              as pack_unitCount
      ,itm.value:package:pack::varchar                               as pack_pack
      ,itm.value:package:name::varchar                               as pack_name
      ,itm.value:package:unitOfMeasurement::varchar                  as pack_UoM
      ,itm.value:measureUnit::varchar                                as measureUnit
      ,itm.value:typeOfUnit::varchar                                 as typeOfUnit
      ,itm.value:vendorItemId::varchar                               as vendorItemId                             
      ,itm.value:type::varchar                                       as type
      ,itm.value:quantity::float                                     as quantity
      ,itm.value:itemClassification::varchar                         as itemClassification
      ,itm.value:summaryItem:discount::float                         as itm_sum_discount
      ,itm.value:summaryItem:price::float                            as itm_sum_price
      ,itm.value:summaryItem:originalPrice::float                    as itm_sum_originalPrice
      ,itm.value:summaryItem:subtotal::float                         as itm_sum_subtotal
      ,itm.value:summaryItem:total::float                            as itm_sum_total
      ,tax.value:id::varchar                                         as tax_id
      ,tax.value:value::float                                        as tax_value
      ,'BEES799'                                                     as SITE_ID
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)
         ,iff(equal_null(itm_key,null),'',itm_key)))                 as KEYCOLS_HASH       -- comment 'Order Key'
      ,md5(concat(SITE_ID
         ,iff(equal_null(orderNumber,null),'',orderNumber)
         ,iff(equal_null(itm_key,null),'',itm_key)
         ,iff(equal_null(sku,null),'',to_varchar(sku))
         ,iff(equal_null(name,null),'',name)
         ,iff(equal_null(pack_itemCount,null),'',to_varchar(pack_itemCount))
         ,iff(equal_null(pack_packageId,null),'',pack_packageId)
         ,iff(equal_null(pack_unitCount,null),'',to_varchar(pack_unitCount))
         ,iff(equal_null(pack_pack,null),'',pack_pack)
         ,iff(equal_null(pack_name,null),'',pack_name)
         ,iff(equal_null(pack_UoM,null),'',pack_UoM)
         ,iff(equal_null(measureUnit,null),'',measureUnit)
         ,iff(equal_null(typeOfUnit,null),'',typeOfUnit)
         ,iff(equal_null(vendorItemId,null),'',vendorItemId)
         ,iff(equal_null(type,null),'',type)
         ,iff(equal_null(quantity,null),'',to_varchar(quantity))
         ,iff(equal_null(itemClassification,null),'',itemClassification)
         ,iff(equal_null(itm_sum_discount,null),'',to_varchar(itm_sum_discount))
         ,iff(equal_null(itm_sum_price,null),'',to_varchar(itm_sum_price))
         ,iff(equal_null(itm_sum_originalPrice,null),'',to_varchar(itm_sum_originalPrice))
         ,iff(equal_null(itm_sum_subtotal,null),'',to_varchar(itm_sum_subtotal))
         ,iff(equal_null(itm_sum_total,null),'',to_varchar(itm_sum_total))
         ,iff(equal_null(tax_id,null),'',tax_id)
         ,iff(equal_null(tax_value,null),'',to_varchar(tax_value)))) as ALLCOLS_HASH       -- comment 'Message Content'
        from EDW_SBX.STG.S1_CDF_BEES_ORDERS
            ,lateral flatten(input => parse_json(MSG_CNTS):order:items, outer => true)  itm
            ,lateral flatten(input => itm.value:summaryItem:taxes, outer => true) tax
     where MSG_TYPE = 'Inbound' and MSG_PROCID=0 and itm_key is not null);


-- insert to S2    
insert into EDW_SBX.INT.S2_CDF_BEES_ITEMS 
    (MSG_KEY          ,MSG_UPDT       ,MSG_UPC
    ,MSG_INT          ,MSG_TYPE       ,orderNumber
    ,itm_key          ,sku            ,name
    ,image            ,pack_itemCount ,pack_packageId
    ,pack_unitCount   ,pack_pack      ,pack_name
    ,pack_UoM
    ,measureUnit      ,typeOfUnit     ,vendorItemId
    ,type             ,quantity       ,itemClassification
    ,itm_sum_discount ,itm_sum_price  ,itm_sum_originalPrice
    ,itm_sum_subtotal ,itm_sum_total
    ,tax_id           ,tax_value
    ,SITE_ID          ,KEYCOLS_HASH    ,ALLCOLS_HASH)
    -- ,MSG_PROCID ,MSG_PROCON
    (select * from temp_S1_CDF_BEES_ITEMS);

    
-- update S1 processed records
 merge into EDW_SBX.STG.S1_CDF_BEES_ORDERS tgt
    using (select distinct MSG_KEY,MSG_UPDT from temp_S1_CDF_BEES_ITEMS) src
    on tgt.MSG_KEY=src.MSG_KEY and tgt.MSG_UPDT=src.MSG_UPDT
    when matched then update set
        tgt.MSG_PROCID=200, tgt.MSG_PROCON=current_timestamp;

        
-- temp S2_CDF_BEES_ITEMS to be inserted to S3 for MSG_TYPE='Outbound'
create or replace temporary table temp_S2_CDF_BEES_ITEMS as
select distinct
     MSG_KEY          ,MSG_UPDT       ,MSG_UPC
    ,MSG_INT          ,MSG_TYPE       ,orderNumber
    ,itm_key          ,sku            ,name 
    ,rank() over (partition by KEYCOLS_HASH order by MSG_INT desc) as LAST_UPDATE
    ,image            ,pack_itemCount ,pack_packageId
    ,pack_unitCount   ,pack_pack      ,pack_name
    ,pack_UoM   
    ,measureUnit      ,typeOfUnit     ,vendorItemId
    ,type             ,quantity       ,itemClassification
    ,itm_sum_discount ,itm_sum_price  ,itm_sum_originalPrice
    ,itm_sum_subtotal ,itm_sum_total   
    ,tax_id           ,tax_value
    ,SITE_ID          ,KEYCOLS_HASH   ,ALLCOLS_HASH
    -- ,MSG_PROCID    ,MSG_PROCON
    from EDW_SBX.INT.S2_CDF_BEES_ITEMS
        where MSG_TYPE='Outbound' and MSG_PROCID is null;


-- merge into S3 for MSG_TYPE='Outbound'              
merge into EDW_SBX.INT.S3_CDF_BEES_ITEMS tgt
    using (select * from temp_S2_CDF_BEES_ITEMS where LAST_UPDATE=1) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH 
    when matched and tgt.ALLCOLS_HASH!= src.ALLCOLS_HASH then update set
         tgt.MSG_KEY=ifnull(src.MSG_KEY,tgt.MSG_KEY)                            ,tgt.MSG_UPDT=ifnull(src.MSG_UPDT,tgt.MSG_UPDT)                   ,tgt.MSG_UPC=ifnull(src.MSG_UPC,tgt.MSG_UPC)
        ,tgt.MSG_INT=ifnull(src.MSG_INT,tgt.MSG_INT)                            ,tgt.MSG_TYPE=ifnull(src.MSG_TYPE,tgt.MSG_TYPE)                   ,tgt.orderNumber=ifnull(src.orderNumber,tgt.orderNumber)
        ,tgt.itm_key=ifnull(src.itm_key,tgt.itm_key)                            ,tgt.sku=ifnull(src.sku,tgt.sku)                                  ,tgt.name=ifnull(src.name,tgt.name)
        ,tgt.image=ifnull(src.image,tgt.image)                                  ,tgt.pack_itemCount=ifnull(src.pack_itemCount,tgt.pack_itemCount) ,tgt.pack_packageId=ifnull(src.pack_packageId,tgt.pack_packageId)
        ,tgt.pack_unitCount=ifnull(src.pack_unitCount,tgt.pack_unitCount)       ,tgt.pack_pack=ifnull(src.pack_pack,tgt.pack_pack)                ,tgt.pack_name=ifnull(src.pack_name,tgt.pack_name)
        ,tgt.pack_UoM=ifnull(src.pack_UoM,tgt.pack_UoM)
        ,tgt.measureUnit=ifnull(src.measureUnit,tgt.measureUnit)                ,tgt.typeOfUnit=ifnull(src.typeOfUnit,tgt.typeOfUnit)              ,tgt.vendorItemId=ifnull(src.vendorItemId,tgt.vendorItemId)
        ,tgt.type=ifnull(src.type,tgt.type)                                     ,tgt.quantity=ifnull(src.quantity,tgt.quantity)                    ,tgt.itemClassification=ifnull(src.itemClassification,tgt.itemClassification)
        ,tgt.itm_sum_discount=ifnull(src.itm_sum_discount,tgt.itm_sum_discount) ,tgt.itm_sum_price=ifnull(src.itm_sum_price,tgt.itm_sum_price)     ,tgt.itm_sum_originalPrice=ifnull(src.itm_sum_originalPrice,tgt.itm_sum_originalPrice)
        ,tgt.itm_sum_subtotal=ifnull(src.itm_sum_subtotal,tgt.itm_sum_subtotal) ,tgt.itm_sum_total=ifnull(src.itm_sum_total,tgt.itm_sum_total) 
        ,tgt.tax_id=ifnull(src.tax_id,tgt.tax_id)                               ,tgt.tax_value=ifnull(src.tax_value,tgt.tax_value)
        ,tgt.SITE_ID=ifnull(src.SITE_ID,tgt.SITE_ID)                            ,tgt.KEYCOLS_HASH=ifnull(src.KEYCOLS_HASH,tgt.KEYCOLS_HASH)        ,tgt.ALLCOLS_HASH=ifnull(src.ALLCOLS_HASH,tgt.ALLCOLS_HASH)
        ,tgt.MSG_PROCID=212                                                     ,tgt.MSG_PROCON=current_timestamp
   when not matched then insert (
         MSG_KEY,          MSG_UPDT,       MSG_UPC
        ,MSG_INT,          MSG_TYPE,       orderNumber
        ,itm_key,          sku,            name
        ,image,            pack_itemCount, pack_packageId
        ,pack_unitCount,   pack_pack,      pack_name
        ,pack_UoM   
        ,measureUnit,      typeOfUnit,    vendorItemId
        ,type,             quantity,      itemClassification
        ,itm_sum_discount, itm_sum_price, itm_sum_originalPrice
        ,itm_sum_subtotal, itm_sum_total
        ,tax_id,           tax_value
        ,SITE_ID,          KEYCOLS_HASH,  ALLCOLS_HASH
        ,MSG_PROCID,       MSG_PROCON)
       values (
         src.MSG_KEY,          src.MSG_UPDT,       src.MSG_UPC
        ,src.MSG_INT,          src.MSG_TYPE,       src.orderNumber
        ,src.itm_key,          src.sku,            src.name
        ,src.image,            src.pack_itemCount, src.pack_packageId
        ,src.pack_unitCount,   src.pack_pack,      src.pack_name
        ,src.pack_UoM   
        ,src.measureUnit,      src.typeOfUnit,     src.vendorItemId
        ,src.type,             src.quantity,       src.itemClassification
        ,src.itm_sum_discount, src.itm_sum_price,  src.itm_sum_originalPrice
        ,src.itm_sum_subtotal, src.itm_sum_total
        ,src.tax_id,           src.tax_value
        ,src.SITE_ID,          src.KEYCOLS_HASH,   src.ALLCOLS_HASH
        ,211,                  current_timestamp);


-- update S2 processed records
merge into EDW_SBX.INT.S2_CDF_BEES_ITEMS tgt
    using (select * from temp_S2_CDF_BEES_ITEMS) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH and tgt.MSG_INT=src.MSG_INT
    when matched then update set
        tgt.MSG_PROCID=210, tgt.MSG_PROCON=current_timestamp;                                                        -- 210 : message has been processed to Order Items as Get


-- temp S2_CDF_BEES_ITEMS to be inserted to S3 for MSG_TYPE='Inbound'
create or replace temporary table temp_S2_CDF_BEES_ITEMS as
select distinct
     MSG_KEY          ,MSG_UPDT       ,MSG_UPC
    ,MSG_INT          ,MSG_TYPE       ,orderNumber
    ,itm_key          ,sku            ,name 
    ,rank() over (partition by KEYCOLS_HASH order by MSG_INT desc) as LAST_UPDATE
    ,image            ,pack_itemCount ,pack_packageId
    ,pack_unitCount   ,pack_pack      ,pack_name
    ,pack_UoM   
    ,measureUnit      ,typeOfUnit     ,vendorItemId
    ,type             ,quantity       ,itemClassification
    ,itm_sum_discount ,itm_sum_price  ,itm_sum_originalPrice
    ,itm_sum_subtotal ,itm_sum_total   
    ,tax_id           ,tax_value
    ,SITE_ID          ,KEYCOLS_HASH   ,ALLCOLS_HASH
    -- ,MSG_PROCID    ,MSG_PROCON
    from EDW_SBX.INT.S2_CDF_BEES_ITEMS
        where MSG_TYPE='Inbound' and MSG_PROCID is null;


-- merge into S3 for MSG_TYPE='Inbound'                 
merge into EDW_SBX.INT.S3_CDF_BEES_ITEMS tgt
    using (select * from temp_S2_CDF_BEES_ITEMS where LAST_UPDATE=1) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH 
    when matched and tgt.ALLCOLS_HASH!= src.ALLCOLS_HASH then update set
         tgt.MSG_KEY=ifnull(src.MSG_KEY,tgt.MSG_KEY)                            ,tgt.MSG_UPDT=ifnull(src.MSG_UPDT,tgt.MSG_UPDT)                   ,tgt.MSG_UPC=ifnull(src.MSG_UPC,tgt.MSG_UPC)
        ,tgt.MSG_INT=ifnull(src.MSG_INT,tgt.MSG_INT)                            ,tgt.MSG_TYPE=ifnull(src.MSG_TYPE,tgt.MSG_TYPE)                   ,tgt.orderNumber=ifnull(src.orderNumber,tgt.orderNumber)
        ,tgt.itm_key=ifnull(src.itm_key,tgt.itm_key)                            ,tgt.sku=ifnull(src.sku,tgt.sku)                                  ,tgt.name=ifnull(src.name,tgt.name)
        ,tgt.image=ifnull(src.image,tgt.image)                                  ,tgt.pack_itemCount=ifnull(src.pack_itemCount,tgt.pack_itemCount) ,tgt.pack_packageId=ifnull(src.pack_packageId,tgt.pack_packageId)
        ,tgt.pack_unitCount=ifnull(src.pack_unitCount,tgt.pack_unitCount)       ,tgt.pack_pack=ifnull(src.pack_pack,tgt.pack_pack)                ,tgt.pack_name=ifnull(src.pack_name,tgt.pack_name)
        ,tgt.pack_UoM=ifnull(src.pack_UoM,tgt.pack_UoM)
        ,tgt.measureUnit=ifnull(src.measureUnit,tgt.measureUnit)                ,tgt.typeOfUnit=ifnull(src.typeOfUnit,tgt.typeOfUnit)              ,tgt.vendorItemId=ifnull(src.vendorItemId,tgt.vendorItemId)
        ,tgt.type=ifnull(src.type,tgt.type)                                     ,tgt.quantity=ifnull(src.quantity,tgt.quantity)                    ,tgt.itemClassification=ifnull(src.itemClassification,tgt.itemClassification)
        ,tgt.itm_sum_discount=ifnull(src.itm_sum_discount,tgt.itm_sum_discount) ,tgt.itm_sum_price=ifnull(src.itm_sum_price,tgt.itm_sum_price)     ,tgt.itm_sum_originalPrice=ifnull(src.itm_sum_originalPrice,tgt.itm_sum_originalPrice)
        ,tgt.itm_sum_subtotal=ifnull(src.itm_sum_subtotal,tgt.itm_sum_subtotal) ,tgt.itm_sum_total=ifnull(src.itm_sum_total,tgt.itm_sum_total) 
        ,tgt.tax_id=ifnull(src.tax_id,tgt.tax_id)                               ,tgt.tax_value=ifnull(src.tax_value,tgt.tax_value)
        ,tgt.SITE_ID=ifnull(src.SITE_ID,tgt.SITE_ID)                            ,tgt.KEYCOLS_HASH=ifnull(src.KEYCOLS_HASH,tgt.KEYCOLS_HASH)        ,tgt.ALLCOLS_HASH=ifnull(src.ALLCOLS_HASH,tgt.ALLCOLS_HASH)
        ,tgt.MSG_PROCID=222                                                     ,tgt.MSG_PROCON=current_timestamp
   when not matched then insert (
         MSG_KEY,          MSG_UPDT,       MSG_UPC
        ,MSG_INT,          MSG_TYPE,       orderNumber
        ,itm_key,          sku,            name
        ,image,            pack_itemCount, pack_packageId
        ,pack_unitCount,   pack_pack,      pack_name
        ,pack_UoM   
        ,measureUnit,      typeOfUnit,    vendorItemId
        ,type,             quantity,      itemClassification
        ,itm_sum_discount, itm_sum_price, itm_sum_originalPrice
        ,itm_sum_subtotal, itm_sum_total
        ,tax_id,           tax_value
        ,SITE_ID,          KEYCOLS_HASH,  ALLCOLS_HASH
        ,MSG_PROCID,       MSG_PROCON)
       values (
         src.MSG_KEY,          src.MSG_UPDT,       src.MSG_UPC
        ,src.MSG_INT,          src.MSG_TYPE,       src.orderNumber
        ,src.itm_key,          src.sku,            src.name
        ,src.image,            src.pack_itemCount, src.pack_packageId
        ,src.pack_unitCount,   src.pack_pack,      src.pack_name
        ,src.pack_UoM   
        ,src.measureUnit,      src.typeOfUnit,     src.vendorItemId
        ,src.type,             src.quantity,       src.itemClassification
        ,src.itm_sum_discount, src.itm_sum_price,  src.itm_sum_originalPrice
        ,src.itm_sum_subtotal, src.itm_sum_total
        ,src.tax_id,           src.tax_value
        ,src.SITE_ID,          src.KEYCOLS_HASH,   src.ALLCOLS_HASH
        ,221,                  current_timestamp);


-- update S2 processed records        
merge into EDW_SBX.INT.S2_CDF_BEES_ITEMS tgt
    using (select * from temp_S2_CDF_BEES_ITEMS) src
    on tgt.KEYCOLS_HASH=src.KEYCOLS_HASH and tgt.MSG_INT=src.MSG_INT
    when matched then update set
        tgt.MSG_PROCID=220, tgt.MSG_PROCON=current_timestamp;                                                          -- 220 : message has been processed to Order Items as Patch


-- clean temp tables...
drop table if exists temp_S1_CDF_BEES_ORDERS;
drop table if exists temp_S2_CDF_BEES_ORDERS;
drop table if exists temp_S1_CDF_BEES_ITEMS;
drop table if exists temp_S2_CDF_BEES_ITEMS;