PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE Stock (
StockID   integer primary key NOT NULL,
Ticker    char(10),
Active    integer,
Name      text,
FYEnds    text,
Beta      real,
HasOption integer,
Close     real,
AvgVol    integer,
Shares    integer,
Floating  integer,
MarketCap integer,
Start     text,
End       text
);
CREATE TABLE Source (
SourceID    integer primary key NOT NULL,
Name        text,
Description text);
CREATE TABLE Sector(
SectorID    integer primary key NOT NULL,
Name        text,
Description text
);
CREATE TABLE Industry(
IndustryID  integer primary key NOT NULL,
Name        text,
Description text
);
CREATE TABLE StockSector (
StockID    integer NOT NULL,
SourceID   integer NOT NULL,
SectorID   integer NOT NULL,
IndustryID integer NOT NULL,
UNIQUE(StockID, SourceID, SectorID, IndustryID) ON CONFLICT REPLACE, 
FOREIGN KEY(StockID)    REFERENCES Stock(StockID),
FOREIGN KEY(SourceID)   REFERENCES Source(SourceID),
FOREIGN KEY(SectorID)   REFERENCES Sector(SectorID),
FOREIGN KEY(IndustryID) REFERENCES Industry(IndustryID));
CREATE TABLE DailyQuota (
StockID         integer NOT NULL,
Date            char(10),
Open            real,
High            real,
Low             real,
Close           real,
Volume          integer,
AdjClose        real,
Amount          real,
ClosePertage    real,
AverageVolum3M  integer,
CorrelationSP3M real,
FOREIGN KEY(StockID)    REFERENCES Stock(StockID));
CREATE TABLE Insider(
InsiderID integer primary key NOT NULL,
Name      text,
Form4Url  text);
CREATE TABLE StockER (
StockID   integer NOT NULL,
FYQuarter char(6),
CYQuarter char(6),
ERDate    char(10),
FOREIGN KEY(StockID) REFERENCES Stock(StockID));
CREATE TABLE InsiderTrans(
StockID     integer NOT NULL,
InsiderID   integer NOT NULL,
Title       text,
Date        char(10),
BuySell     char(1),
Price       real,
Shares      integer,
Amount      real,
FOREIGN KEY(StockID) REFERENCES Stock(StockID),
FOREIGN KEY(InsiderID) REFERENCES Insider(InsiderID));

commit;
