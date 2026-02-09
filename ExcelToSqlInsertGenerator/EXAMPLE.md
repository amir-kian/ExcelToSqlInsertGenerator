# Example: Generate INSERTs from Excel

## 1. Sample INSERT template

Paste this into **Step 1** and click **Parse template**:

```sql
INSERT INTO dbo.MyTable (Id, Name, Status, CreatedAt)
VALUES (
  <Id, uniqueidentifier>,
  <Name, nvarchar(200)>,
  <Status, int>,
  <CreatedAt, datetime>
);
```

The parser finds 4 placeholders: `Id`, `Name`, `Status`, `CreatedAt`.

---

## 2. Load your Excel file

- Click **Browse** and select an Excel file (e.g. `.xlsx`).
- The file should have a header row and columns such as: **Id**, **Name** (or whatever your Excel column names are).

---

## 3. Map each INSERT value

In **Step 3** you map each placeholder to a source:

| INSERT column | SQL type        | Source (Excel or Your own value) | Your own value (when Custom text) |
|---------------|-----------------|-----------------------------------|-----------------------------------|
| Id            | uniqueidentifier| Id                                | *(leave blank)*                   |
| Name          | nvarchar(200)   | Name                              | *(leave blank)*                   |
| Status        | int             | **(Custom text)**                 | **1**                             |
| CreatedAt     | datetime        | **(Custom text)**                 | **GETDATE()**                     |

- **Id**, **Name**: choose the matching Excel columns from the dropdown so values come from the sheet.
- **Status**: select **(Custom text)** and type **1** in "Your own value" — every row will get `1` for Status (like typing in SSMS Edit view).
- **CreatedAt**: select **(Custom text)** and type **GETDATE()** — every row will get the current date/time.

---

## 4. Generate or Execute

- **Generate INSERT script**: builds the full SQL in the text box (you can copy or save).
- **Execute INSERTs**: connects to SQL Server and runs the INSERTs directly (no need to generate first). Use **Execute to database** with your connection string.

---

## Example generated row

For one Excel row with `Id = 'A1B2C3...'` and `Name = 'Product A'`, the generated INSERT will look like:

```sql
INSERT INTO dbo.MyTable (Id, Name, Status, CreatedAt)
VALUES (
  'A1B2C3D4-E5F6-7890-ABCD-EF1234567890',
  N'Product A',
  1,
  '2026-02-10 14:30:00.000'
);
```

`Status` and `CreatedAt` came from **Your own value** (1 and GETDATE()); `Id` and `Name` came from Excel.

---

## If GenderId = 1 then use "Man"

Use the **"When value = ... use"** column (case-style mapping):

1. Map the INSERT column (e.g. **GenderName**) to the Excel column **GenderId** (the one that has 1, 2, etc.).
2. In **When value = ... use** type:
   ```text
   1=N'Man';2=N'Woman';3=N'Other'
   ```
   (Semicolon between rules; each rule is **ExcelValue=SQL expression**.)

Then:
- When the cell value is **1** → the generated SQL uses **N'Man'**.
- When the cell value is **2** → **N'Woman'**.
- When the cell value is **3** → **N'Other'**.
- If the value doesn’t match any rule, the column is converted as usual (e.g. number or string).

So: **if GenderId value = 1 then select Man** is done by mapping that column and setting:
`1=N'Man'` (and optionally `2=N'Woman'` etc.) in **When value = ... use**.
