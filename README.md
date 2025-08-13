## MS Fabric Variable Library

Video demonstration of the MS Fabric Variable Library


## Code Snippets  

Dataflow Gen2  

```
let
  StartDate = Date.From(StartDate),
  
  EndDate = if EndDate = "Current" 
    then Date.EndOfYear(Date.From(DateTime.LocalNow()))
    else Date.From(EndDate),
  
  MaxYear = Date.Year(EndDate),
  
  Dates = Table.FromList(
    List.Dates(
      StartDate, 
      Duration.Days(EndDate-StartDate)+1, 
      #duration(1, 0, 0, 0)
    ),
    Splitter.SplitByNothing(),
    type table[Date=date]
  ),
  
  AddedYear = Table.AddColumn(
    Dates, 
    "Year", 
    each Date.Year([Date]), 
    Int64.Type
  ),
  
  AddedYearCurrent = Table.AddColumn(
      AddedYear, 
      "YearCurrent", 
      each if [Year] = MaxYear 
        then "Current" 
        else Number.ToText([Year]),
      type text
    ), 
  
  AddedQuarter = Table.AddColumn(
    AddedYearCurrent, 
    "Quarter", 
    each Date.QuarterOfYear([Date]), 
    Int64.Type
  ), 
  
  AddedMonth = Table.AddColumn(
    AddedQuarter, 
    "Month", 
    each Date.Month([Date]), 
    Int64.Type
  ),
  
  AddedMonthName = Table.AddColumn(
    AddedMonth, 
    "MonthName", 
    each Date.MonthName([Date]), 
    type text
  ),
  
  ExtractedMonthNameShort = Table.AddColumn(
    AddedMonthName, 
    "MonthNameShort", 
    each Text.Start([MonthName], 3), 
    type text
  )

in
  ExtractedMonthNameShort
```


Data Pipeline parameters array

```json
[{"source":"DimAccount","target":"DimAccount.csv"},
{"source":"DimCompany","target":"DimCompany.csv"},
{"source":"FactIncomeStatement","target":"FactIncomeStatement.csv"}]
```  

DAX Measure  

```DAX
Actual = 
VAR _Default = SUM(FactIncomeStatement[Amount]) 

VAR _Acum = 
    CALCULATE(
        SUM(FactIncomeStatement[Amount]),
        DimAccount[AccountCode] <= MAX(DimAccount[AccountCode]),
        ALL(DimAccount)
    )

RETURN

    SWITCH(
        TRUE(),

        -- If Subgroup and Account are the same and is in the scope of account then returns blank.
        SELECTEDVALUE(DimAccount[AccountName]) = SELECTEDVALUE(DimAccount[AccountSubgroupName]) 
            && ISINSCOPE(DimAccount[AccountName]), 
            BLANK(),

        -- If is a subtotal group and not is in scope of subgroup then returns the _acum constant.
        SELECTEDVALUE(DimAccount[IsSubtotal]) = 1
            && NOT ISINSCOPE(DimAccount[AccountSubgroupName]), 
            _Acum,

        -- Else return the default.
        _Default
    ) 

```  

Use format string `#,0,,.0;(#,0,,.0);-` for the DAX measure.  


## Variables
sql_server
database_name
connection_id
workspace_id
lakehouse_id  
notebook_id  
start_date
end_date


Direct Lake semantic models data source rules 
Server: Lakehouse > Settings > SQL analytics endpoint > SQL connection string  
Database: Open Lakehouse SQL endpoint > Copy the GUID after `mirroredwarehouses/`  
