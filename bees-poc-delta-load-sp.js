// desc procedure STG.CDF_BEES_ORDERS_LOAD_SP

var active="";
var new_rec_check=0;
var db="";


try
{  //try starts

   //begin transaction
   snowflake.execute({sqlText: "begin transaction;"});
   
   // get current database
    var get_db = snowflake.createStatement                                                                
     ({
        sqlText: "select current_database();"
     });
        var get_db_rs = get_db.execute();
            get_db_rs.next();
            db = get_db_rs.getColumnValue(1);
    
    // check if there is a new record in staging
    var new_rec_check_stmt = snowflake.createStatement
     ({
        sqlText: "select count(1) as NEW_REC from EDW_SBX.STG.S1_CDF_BEES_ORDERS where MSG_PROCID is null;"
     });
        var new_rec_check_rs = new_rec_check_stmt.execute();
            new_rec_check_rs.next();
            new_rec_check = new_rec_check_rs.getColumnValue(1);
       
              
    //commit transaction if all completed
    snowflake.execute({sqlText: "commit;"});         
    return "success";   

}

// return success/error indicator              
catch(err)
{
    //roll back transaction if there is any error.
     snowflake.execute({sqlText: "rollback;"});
    //set final error message to load into load log 
     var error_message = err.message.replace(/['']/g,"''").substring(0,380);
    //return error_message;
     return error_message;
     
}
