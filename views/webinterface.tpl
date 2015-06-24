<html>
<head>
<!-- Latest compiled JavaScript -->
<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
<!--
c_id
company
company_r
consumer_d
data_s
date_r
issue
product
state
subissue
subproduct
timely_r
via
zip
 -->
<script>
$( document ).ready(function() {
    console.log( "ready!" );
               $( "#show" ).click(function() {
                        var postObj = {};
                        postObj["c_id"] = $( "#c_id" ).val();
                        postObj["company"] = $( "#company" ).val();
                        postObj["product"] = $( "#product" ).val();
                        postObj["zip"] = $( "#zip" ).val();
                        postObj["state"] = $( "#state" ).val();
                        postObj["via"] = $( "#via" ).val();
                        console.log(postObj);
                        var startTime= new Date();
               			$.post("/querybuilder",postObj,
    				function(data, status){
        				//alert("Data: " + data + "\nStatus: " + status);
                                	//$("#content").text(data);
                                        addContent(data);
                                        var totalTime = new Date()-startTime;
                                        $("#timetaken").html("<b>TimeTaken to execute query = "+totalTime+" Milliseconds</b>");
               			});

		});
    function addContent(resp){
      var resp = JSON.parse(resp);
      buildHtmlTable(resp)
    }


//http://stackoverflow.com/questions/5180382/convert-json-data-to-a-html-table   coverting json list to table 

    function buildHtmlTable(myList) {
     $("#excelDataTable").html('');
    var columns = addAllColumnHeaders(myList);
    for (var i = 0 ; i < myList.length ; i++) {
        var row$ = $('<tr/>');
        for (var colIndex = 0 ; colIndex < columns.length ; colIndex++) {
            var cellValue = myList[i][columns[colIndex]];

            if (cellValue == null) { cellValue = ""; }

            row$.append($('<td/>').html(cellValue));
        }
        $("#excelDataTable").append(row$);
    }
}

// Adds a header row to the table and returns the set of columns.
// Need to do union of keys from all records as some records may not contain
// all records
function addAllColumnHeaders(myList)
{
    var columnSet = [];
    var headerTr$ = $('<tr/>');

    for (var i = 0 ; i < myList.length ; i++) {
        var rowHash = myList[i];
        for (var key in rowHash) {
            if ($.inArray(key, columnSet) == -1){
                columnSet.push(key);
                headerTr$.append($('<th/>').html(key));
            }
        }
    }
    $("#excelDataTable").append(headerTr$);

    return columnSet;
}



});

</script>
</head>
<body>
<h1>
Welcome to the web interface of consumer complaints database  
</h1>
<h2>
Enter the search criteria : 
</h2>
consumer id : <input type='text' id='c_id'><br>
company     : <input type='text' id='company'><br>
product     : <input type='text' id='product'><br>
zip         : <input type='text' id='zip'><br>
state         : <input type='text' id='state'><br>
via         : <input type='text' id='via'><br>
<input type="button" id="show" value="show">
<div id="timetaken"> </div>
<div id="content">
<table id="excelDataTable" border="1">
  </table>
</div>
</body>
<html>
