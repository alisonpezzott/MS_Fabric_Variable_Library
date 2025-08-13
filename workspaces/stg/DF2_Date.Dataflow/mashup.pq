[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared StartDate = "2023-01-01" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared EndDate = "2024-02-28" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "DimDate_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared DimDate = let
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
  ExtractedMonthNameShort;
shared DimDate_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd7878ee-2fcb-4af2-a47a-a221a04ebabd"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "74874010-1741-4366-a3a1-3e03f231c0a5"]}[Data],
  TableNavigation = Navigation_2{[Id = "DimDate", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
