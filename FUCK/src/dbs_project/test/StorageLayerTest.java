package dbs_project.test;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.primitives.ArrayIntList;

import dbs_project.abstraction.GenericLinearReadAccessible;
import dbs_project.exceptions.*;
import dbs_project.storage.Column;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.StorageLayer;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import dbs_project.storage.Type;
import dbs_project.storage.implIsaac.ColumnCST;
import dbs_project.storage.implIsaac.ColumnMetaDataCST;
import dbs_project.storage.implIsaac.RowCST;
import dbs_project.storage.implIsaac.RowCursorCST;
import dbs_project.storage.implIsaac.RowMetaDataCST;
import dbs_project.storage.impl.StorageLayerSMMDS;
import dbs_project.structures.DataStructure;
import dbs_project.structures.LinearDataStructure;
import dbs_project.structures.LinearList;

public class StorageLayerTest {
	
	StorageLayer storage;
	public static final String TABLE_NAME_1 = "table_1";
    public static final String TABLE_NAME_2 = "table_2";
    public static final String TABLE_NAME_3 = "table_3";
    public static final List<String> TABLE_NAMES = Arrays.asList(new String[]{StorageLayerTest.TABLE_NAME_1, StorageLayerTest.TABLE_NAME_2, StorageLayerTest.TABLE_NAME_3});

    //
    private final Logger log;
    //
    private int cantidadErrores;
	
	public StorageLayerTest(){
		storage = new StorageLayerSMMDS();
		this.log = Logger.getLogger("StorageLayerTest.java");
		this.cantidadErrores = 0;
	}
	
	//STORAGE LAYER TESTS//
	
	public void testTableNameDuplication() {
        System.out.println("testTableNameDuplication");
        storage = new StorageLayerSMMDS();

        // initialize table schema
        Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("aColumn", Type.STRING);


        // create 1 new table
        try {
            storage.createTable(TABLE_NAME_1, tableSchema, DataStructure.LINKEDLIST);
        } catch (TableAlreadyExistsException e) {
            fail("Table creation failed: table " + TABLE_NAME_1 + " couldn't be created");
        }

        try { // create table with duplicate name
            storage.createTable(TABLE_NAME_1, tableSchema, DataStructure.LINKEDLIST);
            fail("Should have received an exception when adding a duplicate table: table " + TABLE_NAME_1 + " already exist");

        } catch (TableAlreadyExistsException ex) {
            // this is what we want
        }
    }
	
	 public void testRenameTable() {
        System.out.println("testRenameTable");
        storage = new StorageLayerSMMDS();
        
        // initialize table schema (empty schema)
        final Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("aColumn", Type.STRING);

        int tableUID = 0;
        // create 1 new table
        try {
            tableUID = storage.createTable(TABLE_NAME_1, tableSchema, DataStructure.LINKEDLIST);
        } catch (TableAlreadyExistsException e) {
            fail("Table " + TABLE_NAME_1 + " must not exist as the database is empty");
        }

        // rename table
        try {
            storage.renameTable(tableUID, TABLE_NAME_2);
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_1 + " must exist");
        } catch (TableAlreadyExistsException e) {
            fail("It must be possible to rename table " + TABLE_NAME_1);
        }

        // get table name
        String newTableNameCheck = null;
        try {
            newTableNameCheck = storage.getTable(tableUID).getTableMetaData().getName();
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_2 + " must exist");
        }

        assertEquals("table names not equal", TABLE_NAME_2, newTableNameCheck);

        // take back to original state
        try {
            storage.renameTable(tableUID, TABLE_NAME_1);

        } catch (NoSuchTableException ex) {
            fail("Table " + TABLE_NAME_2 + " must exist");
        } catch (TableAlreadyExistsException e) {
            fail("It must be possible to rename table " + TABLE_NAME_2);

        }
    }
	 
	 public void testCreateTable() throws Exception {
	 	System.out.println("testCreateTable");
	 	storage = new StorageLayerSMMDS();
	 	
        // initialize table schema
        final Map<String, Type> tableSchemaMap = new HashMap<String, Type>();
        tableSchemaMap.put("colInteger", Type.INTEGER);
        tableSchemaMap.put("colDouble", Type.DOUBLE);
        tableSchemaMap.put("colBoolean", Type.BOOLEAN);
        tableSchemaMap.put("colString", Type.STRING);
        tableSchemaMap.put("colDate", Type.DATE);
        tableSchemaMap.put("colInteger2", Type.INTEGER);
        tableSchemaMap.put("colDouble2", Type.DOUBLE);
        tableSchemaMap.put("colBoolean2", Type.BOOLEAN);
        tableSchemaMap.put("colString2", Type.STRING);
        tableSchemaMap.put("colDate2", Type.DATE);

        // create 1 new table
        final int tableUID = storage.createTable(TABLE_NAME_1, tableSchemaMap, DataStructure.LINKEDLIST);
        //check invariants
        final Table createdTable = checkTableSchemaInvariants(tableUID, TABLE_NAME_1, tableSchemaMap);
        final RowCursor allRows = createdTable.getRows(DataStructure.LINKEDLIST);
        //should be empty, nothing inserted yet
        assertEquals("empty table has row count != 0", createdTable.getTableMetaData().getRowCount(), 0);
        assertFalse("emtpy table delivered rows", allRows.next());
        allRows.close();
        //check create table with empty schema, which should be supported
        storage.createTable(TABLE_NAME_2, new HashMap<String, Type>(), DataStructure.LINKEDLIST);
    }
	 
	 private Table checkTableSchemaInvariants(int tableID, String tableName, Map<String, Type> schema) throws NoSuchTableException {
        //find right table
        final Table table = storage.getTable(tableID);
        final TableMetaData metaData = table.getTableMetaData();
        //retrieved table has right id?
        assertEquals("table id inconsistent", metaData.getId(), tableID);
        //has right name?
        assertEquals("table name inconsistent", metaData.getName(), tableName);
        //has right column count according to schema?
        assertEquals("column count inconsistent", metaData.getTableSchema().size(), schema.size());
        //delivers right columns?
        int columnCount = 0;
        final int rowCount = metaData.getRowCount();
        final ColumnCursor allColumns = table.getColumns(DataStructure.LINKEDLIST);
        final Map<String, ColumnMetaData> tableSchema = metaData.getTableSchema();
        while (allColumns.next()) {
            ++columnCount;
            final ColumnMetaData currentColMeta = allColumns.getMetaData();
            final String columnName = currentColMeta.getName();
            //expected type?
            assertEquals("type inconsistent", currentColMeta.getType(), schema.get(columnName));
            //expected source table?
            assertEquals("tableId inconsistent", tableID, currentColMeta.getSourceTable().getTableMetaData().getId());
            //expected column label?
            assertEquals("table name inconsistent", table.getTableMetaData().getName() + "." + columnName, currentColMeta.getLabel());
            //
            assertTrue("table schema inconsistent", isColumnMetaDataEqual(currentColMeta, tableSchema.get(columnName)));
            
            //has column the right row count for this table?
            if (rowCount >= 0) {
                assertEquals("column's row count inconsistent with table", rowCount, currentColMeta.getRowCount());
            }
        }
        //delivered right amount of columns?
        assertEquals("column count inconsistent", columnCount, schema.size());
        allColumns.close();
        return table;
    }
	 
	 private boolean isColumnMetaDataEqual(ColumnMetaData c1, ColumnMetaData c2) {
        boolean  resultado = c1 == c2
                || (c1.getId() == c2.getId()
                && c1.getRowCount() == c2.getRowCount()
                && c1.getType().equals(c2.getType())
                && c1.getName().equals(c2.getName())
                && c1.getLabel().equals(c2.getLabel())
                && c1.getSourceTable().equals(c2.getSourceTable()));
        return resultado;
    }	
	 
	 public void testDeleteTable() throws Exception {
        System.out.println("testDeleteTable");
        storage = new StorageLayerSMMDS();

        // initialize table schema (empty schema)
        final Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("col1", Type.STRING);
        tableSchema.put("col2", Type.DATE);

        int tableUID = 0;

        // create 1 new table
        try {
            tableUID = storage.createTable(TABLE_NAME_1, tableSchema, DataStructure.LINKEDLIST);
            assertEquals("wrong number of tables in db", storage.getTables(DataStructure.LINKEDLIST).size(), 1);
        } catch (TableAlreadyExistsException e) {
            fail("Table " + TABLE_NAME_1 + " must not exist as the database is empty");
        }

        try {
            Table table = storage.getTable(tableUID);
            assertEquals("inconsistent table id", table.getTableMetaData().getId(), tableUID);
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_1 + " must exist");
        }

        // delete table instance
        try {
            storage.deleteTable(tableUID);
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_1 + " should have been deleted");
        }

        try {
            storage.deleteTable(tableUID);
            fail("Should have received an exception when deleting non-existing table " + TABLE_NAME_1);
        } catch (NoSuchTableException ex) {
            //expected
        }
        assertTrue("wrong number of tables in db", storage.getTables(DataStructure.LINKEDLIST).isEmpty());
    }
	 
	 public void testGetTables() throws Exception {
        System.out.println("testGetTables");
        storage = new StorageLayerSMMDS();
        
        final Map<String, Type> tableSchemaMap1 = new HashMap<String, Type>();
        tableSchemaMap1.put("colInteger", Type.INTEGER);
        tableSchemaMap1.put("colDouble", Type.DOUBLE);
        tableSchemaMap1.put("colString", Type.STRING);
        tableSchemaMap1.put("colString2", Type.STRING);
        final Map<String, Type> tableSchemaMap2 = new HashMap<String, Type>();
        tableSchemaMap2.put("colInteger", Type.INTEGER);
        tableSchemaMap2.put("colBoolean", Type.BOOLEAN);
        tableSchemaMap2.put("colDate", Type.DATE);
        final Map<String, Type> tableSchemaMap3 = new HashMap<String, Type>();
        tableSchemaMap3.put("colString", Type.STRING);
        tableSchemaMap3.put("colDate", Type.DATE);
        final int id1 = storage.createTable(TABLE_NAME_1, tableSchemaMap1, DataStructure.LINKEDLIST);
        final int id2 = storage.createTable(TABLE_NAME_2, tableSchemaMap2, DataStructure.LINKEDLIST);
        final int id3 = storage.createTable(TABLE_NAME_3, tableSchemaMap3, DataStructure.LINKEDLIST);
        final Map<Integer, String> idToName = new HashMap<Integer, String>(3);
        idToName.put(id1, TABLE_NAME_1);
        idToName.put(id2, TABLE_NAME_2);
        idToName.put(id3, TABLE_NAME_3);
        final LinearDataStructure<? extends Table> tables = storage.getTables(DataStructure.LINKEDLIST);
        assertEquals("wrong table count in db", tables.size(), idToName.size());
        @SuppressWarnings("unchecked")
		LinearList<Table> listaTablas = (LinearList<Table>) tables;
        int i = 0;
        for(listaTablas.goToStart(); i < listaTablas.size(); listaTablas.next(), i++){
        	final String nameFound = idToName.get(listaTablas.getElement().getTableMetaData().getId());
        	assertEquals("wrong table name found", nameFound, listaTablas.getElement().getTableMetaData().getName());
        }
        /*for (final Table table : tables) {
            final String nameFound = idToName.get(table.getTableMetaData().getId());
            assertEquals("wrong table name found", nameFound, table.getTableMetaData().getName());
        }*/
    }
	 
	//TABLE TESTS//
	 public void testRenameColumn() throws Exception {
        System.out.println("testRenameColumn");
        storage = new StorageLayerSMMDS();
        
        final Map<String, TableCreationResult> tables = this.createTables(StorageLayerTest.TABLE_NAMES, storage);
        final Table table1 = tables.get(StorageLayerTest.TABLE_NAME_1).getTable();
        final String oldColName = "colString";
        final String otherExistingColName = "colInteger";
        final String newColName = "colVarchar";
        //get the test column 
        final Column testColumn = this.getColumnByName(table1, oldColName);
        assertTrue("la columna no se encontro", testColumn != null);
        if(testColumn == null){
        	throw new NullPointerException();
        }
        final int testColumnId = testColumn.getMetaData().getId();
        //has the column the right name?
        assertEquals("column with wrong name returned", testColumn.getMetaData().getName(), oldColName);
        try {
            //rename to other existing name which should fail
            table1.renameColumn(testColumnId, otherExistingColName);
            fail("column should already exist!");
        } catch (ColumnAlreadyExistsException ex) {
            //expected
        }
        //rename the column to new name
        table1.renameColumn(testColumnId, newColName);
        //invariants
        //new name propagated to meta data?
        assertEquals("renamed column delivers wrong name through meta data", testColumn.getMetaData().getName(), newColName);
        //id still remains the same?
        assertEquals("rename operation has changed column id", testColumn.getMetaData().getId(), testColumnId);
        //we also find an up to date version when asking the table?
        assertEquals("renamed column delivers wrong name through meta data", table1.getColumn(testColumnId).getMetaData().getName(), newColName);
        //label is updated?
        //assertEquals("renamed column delivers wrong label through meta data", testColumn.getMetaData().getLabel(), table1.getTableMetaData().getName() + "." + newColName);
        //same source table?
        assertEquals("renamed column delivers wrong source table through meta data", testColumn.getMetaData().getSourceTable().getTableMetaData().getId(), table1.getTableMetaData().getId());
        //rename back to old name works?
        table1.renameColumn(testColumnId, oldColName);
        assertEquals("re-renamed column delivers wrong name through meta data", testColumn.getMetaData().getName(), oldColName);
    }

	 public void testCreateColumn() throws Exception {
        System.out.println("testCreateColumn");
        storage = new StorageLayerSMMDS();
        
        final Map<String, TableCreationResult> createResults = this.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        final String otherExistingColName = "colInteger";
        final String newColName = "newColumnInteger";
        final TableCreationResult tableCreationResult1 = createResults.get(StorageLayerTest.TABLE_NAME_1);
        final Table table1 = tableCreationResult1.getTable();
        try {
            table1.createColumn(otherExistingColName, Type.STRING);
            fail("column should already exist");
        } catch (ColumnAlreadyExistsException ex) {
            //expected
        }
        final int newColumnId = table1.createColumn(newColName, Type.INTEGER);
        final Column newColumn = table1.getColumn(newColumnId);
        final ColumnMetaData columnMetaData = newColumn.getMetaData();
        //invariants
        assertEquals("both ids should be the same", columnMetaData.getId(), newColumnId);
        assertEquals("both names should be the same",columnMetaData.getName(), newColName);
        assertEquals("both labels should be the same",columnMetaData.getLabel(), table1.getTableMetaData().getName() + "." + newColName);
        //System.out.println("*" + columnMetaData.getRowCount() + ";" +  table1.getTableMetaData().getRowCount());
        assertEquals("both row count should be the same",columnMetaData.getRowCount(), table1.getTableMetaData().getRowCount());
        assertEquals("both source table should be the same",columnMetaData.getSourceTable().getTableMetaData().getId(), table1.getTableMetaData().getId());
    }
	 
	 public void testAddAndGetRows() throws Exception {
        System.out.println("testAddAndGetRows");
        storage = new StorageLayerSMMDS();
        
        //test addRows()
        final Map<String, TableCreationResult> tableCreatResults = this.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : tableCreatResults.values()) {
            final Table toTestTable = artifacts.getTable();
            final ArrayIntList idList = artifacts.getGeneratedIds();
            assertEquals("row count should be the same", toTestTable.getTableMetaData().getRowCount(), idList.size());
            //test getRows
            final RowCursor toTestCursor = toTestTable.getRows(DataStructure.LINKEDLIST, idList.iterator());
            final RowCursor referenceCursor = artifacts.getRowCursor();
            assertTrue("result is different from reference: addAndgetRows", compareRowCursors(referenceCursor, toTestCursor));
            referenceCursor.close();
            toTestCursor.close();
        }
    }
	 
	 public void testDeleteRows() throws Exception {
        System.out.println("testDeleteRows");
        storage = new StorageLayerSMMDS();
        
        final Map<String, TableCreationResult> tableCreateResults = this.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : tableCreateResults.values()) {
            final Table toTestTable = artifacts.getTable();
            final ArrayIntList idList = artifacts.getGeneratedIds();
            
            //test
            assertEquals("before deleting, row count should be equal", toTestTable.getTableMetaData().getRowCount(), idList.size());
            
            //delete first row
            toTestTable.deleteRow(idList.get(0));
            idList.removeElementAt(0);
            assertEquals("after deleting, row count should be equal", toTestTable.getTableMetaData().getRowCount(), idList.size());
            
            //test cursor from remaining rows
            final RowCursor toTestCursor = toTestTable.getRows(DataStructure.LINKEDLIST, idList.iterator());
            final RowCursor referenceCursor = artifacts.getRowCursorTestDelete();
            assertTrue("result is different from reference: deleteRows",compareRowCursors(referenceCursor, toTestCursor));
            
        }
    }
	 
	 public void testDropColumns() throws Exception {
        System.out.println("testDropColumns");
        storage = new StorageLayerSMMDS();
        StorageLayerSMMDS storage2 = new StorageLayerSMMDS();
        
        Map<String, TableCreationResult> tableCreationResultMapReference = this.createTablesAndAddRowsTestDropTable(StorageLayerTest.TABLE_NAMES, storage2);
        
        final Map<String, TableCreationResult> tableCreationResultMap = this.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : tableCreationResultMap.values()) {
            final Table toTestTable = artifacts.getTable();
            final ArrayIntList idList = artifacts.getGeneratedIds();
            
            //compare before dropping columns
            assertEquals("there should be 5 columns", toTestTable.getTableMetaData().getTableSchema().size(), 5);
            
            //drop the column for double
            int idColumnDouble = this.getColumnByName(toTestTable, "colDouble").getMetaData().getId();
            toTestTable.dropColumn(idColumnDouble);
            //compare after dropping one column
            assertEquals("there should be 4 columns", toTestTable.getTableMetaData().getTableSchema().size(), 4);
          //drop the column for date
            int idColumnDate = this.getColumnByName(toTestTable, "colDate").getMetaData().getId();
            toTestTable.dropColumn(idColumnDate);
            //compare after dropping one column
            assertEquals("there should be 3 columns", toTestTable.getTableMetaData().getTableSchema().size(), 3);
            
            //test that the remaining columns are the same
            final RowCursor toTestCursor = toTestTable.getRows(DataStructure.LINKEDLIST, idList.iterator());
            final RowCursor referenceCursor = tableCreationResultMapReference.get(toTestTable.getTableMetaData().getName()).getRowCursor();
            assertTrue("result is different from reference: dropColumns",compareRowCursors(referenceCursor, toTestCursor));
            
    					
        }
    }
	 
	 //TEST FOR GenericLinearReadAccessible 
	 public void testCastsColumn() throws Exception {
        System.out.println("testCastsColumn");
        storage = new StorageLayerSMMDS();
        
        final Map<String, TableCreationResult> createdTables = this.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : createdTables.values()) {
            final Table testTable = artifacts.getTable();
            final ColumnCursor columnCursor = testTable.getColumns(DataStructure.LINKEDLIST);
            testForColumnCursor(columnCursor);
        }
    }
	 
 public void testCastsRow() throws Exception {
        System.out.println("testCastsRow");
        storage = new StorageLayerSMMDS();
        
        final Map<String, TableCreationResult> createdTables = this.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : createdTables.values()) {
            final Table testTable = artifacts.getTable();
            final RowCursor rowCursor = testTable.getRows(DataStructure.LINKEDLIST);
            testForRowCursor(rowCursor);
        }
    }
	 
	//PRIVATE METHODS//
	
	 private void fail(String error){
		this.log.log(Level.SEVERE, error);
		this.cantidadErrores++;
	}
	
	private void assertEquals(String msg, Object val1, Object val2){
		
		if (!val1.equals(val2)){
			//this.log.log(Level.INFO, "comparando: " + val1.toString() + " con " + val2.toString());
			log.log(Level.SEVERE, msg + " Objeto1: " + val1 + ", Objeto2: " + val2);
			this.cantidadErrores++;
		}
	}

	private void assertTrue(String msg, boolean valor){
		if (!valor){
			log.log(Level.SEVERE, msg);
			this.cantidadErrores++;
		}
	}
	
	private void assertFalse(String msg, boolean valor){
		if (valor){
			log.log(Level.SEVERE, msg);
			this.cantidadErrores++;
		}
	}
	
	public void cantidadErrores(){
		
		System.out.println("****** La cantidad de errores hasta el momento fueron: " + this.cantidadErrores);
	}
	
	private Map<String,TableCreationResult> createTables(List<String> tables, StorageLayer storage) throws TableAlreadyExistsException, NoSuchTableException{
		
		//Store tables
		Map<String,TableCreationResult> result = new HashMap<String,TableCreationResult>();
		
		// initialize table schema (5 columns)
        final Map<String, Type> tableSchemaMap = new HashMap<String, Type>();
        tableSchemaMap.put("colInteger", Type.INTEGER);
        tableSchemaMap.put("colDouble", Type.DOUBLE);
        tableSchemaMap.put("colBoolean", Type.BOOLEAN);
        tableSchemaMap.put("colString", Type.STRING);
        tableSchemaMap.put("colDate", Type.DATE);
        
		ListIterator<String> tablas = tables.listIterator();
		while(tablas.hasNext()){
			String tableName = tablas.next();
			int idTable = storage.createTable(tableName, tableSchemaMap, DataStructure.LINKEDLIST);
			TableCreationResult tableCreationResult = new TableCreationResult(storage.getTable(idTable),null,null);
			result.put(tableName,tableCreationResult);
		}
		
		return result;
	}
	
private Map<String,TableCreationResult> createTablesTestDropTable(List<String> tables, StorageLayer storage) throws TableAlreadyExistsException, NoSuchTableException{
		
		//Store tables
		Map<String,TableCreationResult> result = new HashMap<String,TableCreationResult>();
		
		// initialize table schema (3 columns)
        final Map<String, Type> tableSchemaMap = new HashMap<String, Type>();
        tableSchemaMap.put("colInteger", Type.INTEGER);
        tableSchemaMap.put("colBoolean", Type.BOOLEAN);
        tableSchemaMap.put("colString", Type.STRING);
        
		ListIterator<String> tablas = tables.listIterator();
		while(tablas.hasNext()){
			String tableName = tablas.next();
			int idTable = storage.createTable(tableName, tableSchemaMap, DataStructure.LINKEDLIST);
			TableCreationResult tableCreationResult = new TableCreationResult(storage.getTable(idTable),null,null);
			result.put(tableName,tableCreationResult);
		}
		
		return result;
	}
	
	private Map<String,TableCreationResult> createTablesAndAddRows(List<String> tables, StorageLayer storage) throws TableAlreadyExistsException, NoSuchTableException, SchemaMismatchException{
		Map<String,TableCreationResult> result = createTables(tables, storage);
		
		// IMPORTANTE! Aqui se esta utilizando una implementacion propia del profesor de Row (RowCST), RowMetaData (RowMetaDataCST), 
		// ColumnMetaData (ColumnMetaDataCST), estan implementaciones son para poder probar su proyecto, no lo debe cambiar!
		
		//create Rows (2 rows)
		//Row 1
		Vector<Object> rowData1 = new Vector<Object>();
		rowData1.add(new Integer(1));
		rowData1.add(new Double(1.1));
		rowData1.add(new Boolean(true));
		rowData1.add(new String("Isaac Alpizar @ CRC"));
		rowData1.add(new Date());;
		ColumnMetaData  r1C1 = new ColumnMetaDataCST(-1,null, -1,"colInteger", "", Type.INTEGER, null, null); 
		ColumnMetaData  r1C2 = new ColumnMetaDataCST(-1,null, -1,"colDouble", "", Type.DOUBLE, null, null);
		ColumnMetaData  r1C3 = new ColumnMetaDataCST(-1,null, -1,"colBoolean", "", Type.BOOLEAN, null, null);
		ColumnMetaData  r1C4 = new ColumnMetaDataCST(-1,null, -1,"colString", "", Type.STRING, null, null);
		ColumnMetaData  r1C5 = new ColumnMetaDataCST(-1,null, -1,"colDate", "", Type.DATE, null, null);
		Vector<ColumnMetaData> columnsMetaData1 = new Vector<ColumnMetaData>();
		columnsMetaData1.add(r1C1);
		columnsMetaData1.add(r1C2);
		columnsMetaData1.add(r1C3);
		columnsMetaData1.add(r1C4);
		columnsMetaData1.add(r1C5);
		RowMetaDataCST rowMetaData1 = new RowMetaDataCST(-1, 5, columnsMetaData1);
		RowCST row1 = new RowCST(rowData1, rowMetaData1);
		//Row 2
		Vector<Object> rowData2 = new Vector<Object>();
		rowData2.add(new Integer(2));
		rowData2.add(new Double(2.2));
		rowData2.add(new Boolean(false));
		rowData2.add(new String("Harry Clifton @ UK"));
		rowData2.add(new Date());;
		ColumnMetaData  r2C1 = new ColumnMetaDataCST(-1,null, -1,"colInteger", "", Type.INTEGER, null, null); 
		ColumnMetaData  r2C2 = new ColumnMetaDataCST(-1,null, -1,"colDouble", "", Type.DOUBLE, null, null);
		ColumnMetaData  r2C3 = new ColumnMetaDataCST(-1,null, -1,"colBoolean", "", Type.BOOLEAN, null, null);
		ColumnMetaData  r2C4 = new ColumnMetaDataCST(-1,null, -1,"colString", "", Type.STRING, null, null);
		ColumnMetaData  r2C5 = new ColumnMetaDataCST(-1,null, -1,"colDate", "", Type.DATE, null, null);
		Vector<ColumnMetaData> columnsMetaData2 = new Vector<ColumnMetaData>();
		columnsMetaData2.add(r2C1);
		columnsMetaData2.add(r2C2);
		columnsMetaData2.add(r2C3);
		columnsMetaData2.add(r2C4);
		columnsMetaData2.add(r2C5);
		RowMetaDataCST rowMetaData2 = new RowMetaDataCST(-1, 5, columnsMetaData2);
		RowCST row2 = new RowCST(rowData2, rowMetaData2);
		
		Iterator<TableCreationResult> tablas = result.values().iterator();
		while(tablas.hasNext()){
			TableCreationResult tableResult = tablas.next();
			Table tabla = tableResult.getTable();
			ArrayIntList ids = new ArrayIntList();
			ids.add(tabla.addRow(row1));
			ids.add(tabla.addRow(row2));
			tableResult.setGeneratedIds(ids);
			RowCST[] rArray = new RowCST[2];
			rArray[0] = row1;
			rArray[1] = row2;
			tableResult.setRows(rArray);
			
		}
		
		return result;
	}
	
	private Map<String,TableCreationResult> createTablesAndAddRowsTestDropTable(List<String> tables, StorageLayer storage) throws TableAlreadyExistsException, NoSuchTableException, SchemaMismatchException{
		Map<String,TableCreationResult> result = createTablesTestDropTable(tables, storage);
		
		// IMPORTANTE! Aqui se esta utilizando una implementacion propia del profesor de Row (RowCST), RowMetaData (RowMetaDataCST), 
		// ColumnMetaData (ColumnMetaDataCST), estan implementaciones son para poder probar su proyecto, no lo debe cambiar!
		
		//create Rows (2 rows)
		//Row 1
		Vector<Object> rowData1 = new Vector<Object>();
		rowData1.add(new Integer(1));
		rowData1.add(new Boolean(true));
		rowData1.add(new String("Isaac Alpizar @ CRC"));
		ColumnMetaData  r1C1 = new ColumnMetaDataCST(-1,null, -1,"colInteger", "", Type.INTEGER, null, null); 
		ColumnMetaData  r1C3 = new ColumnMetaDataCST(-1,null, -1,"colBoolean", "", Type.BOOLEAN, null, null);
		ColumnMetaData  r1C4 = new ColumnMetaDataCST(-1,null, -1,"colString", "", Type.STRING, null, null);
		Vector<ColumnMetaData> columnsMetaData1 = new Vector<ColumnMetaData>();
		columnsMetaData1.add(r1C1);
		columnsMetaData1.add(r1C3);
		columnsMetaData1.add(r1C4);
		RowMetaDataCST rowMetaData1 = new RowMetaDataCST(-1, 3, columnsMetaData1);
		RowCST row1 = new RowCST(rowData1, rowMetaData1);
		//Row 2
		Vector<Object> rowData2 = new Vector<Object>();
		rowData2.add(new Integer(2));
		rowData2.add(new Boolean(false));
		rowData2.add(new String("Harry Clifton @ UK"));
		ColumnMetaData  r2C1 = new ColumnMetaDataCST(-1,null, -1,"colInteger", "", Type.INTEGER, null, null); 
		ColumnMetaData  r2C3 = new ColumnMetaDataCST(-1,null, -1,"colBoolean", "", Type.BOOLEAN, null, null);
		ColumnMetaData  r2C4 = new ColumnMetaDataCST(-1,null, -1,"colString", "", Type.STRING, null, null);
		Vector<ColumnMetaData> columnsMetaData2 = new Vector<ColumnMetaData>();
		columnsMetaData2.add(r2C1);
		columnsMetaData2.add(r2C3);
		columnsMetaData2.add(r2C4);
		RowMetaDataCST rowMetaData2 = new RowMetaDataCST(-1, 3, columnsMetaData2);
		RowCST row2 = new RowCST(rowData2, rowMetaData2);
		
		Iterator<TableCreationResult> tablas = result.values().iterator();
		while(tablas.hasNext()){
			TableCreationResult tableResult = tablas.next();
			Table tabla = tableResult.getTable();
			ArrayIntList ids = new ArrayIntList();
			ids.add(tabla.addRow(row1));
			ids.add(tabla.addRow(row2));
			tableResult.setGeneratedIds(ids);
			RowCST[] rArray = new RowCST[2];
			rArray[0] = row1;
			rArray[1] = row2;
			tableResult.setRows(rArray);
			
		}
		
		return result;
	}
	
	private Column getColumnByName(Table table, String colName) throws NoSuchColumnException{
		ColumnCursor columnas = table.getColumns(DataStructure.LINKEDLIST);
		while(columnas.next()){
			if (columnas.getMetaData().getName().equals(colName)){
				return table.getColumn(columnas.getMetaData().getId());
			}
		}
		return null;
	}
	
	private boolean compareRowCursors(RowCursor reference, RowCursor toCompare) throws Exception {
        assertTrue("Reference result is null!", reference != null);
        assertTrue("Result to check is null!", toCompare != null);
        
        final MappingInfo mappingInfo = getFieldMappingFromMetaData(reference, toCompare, false);
        if (mappingInfo != null) {
            final int[] mapping = mappingInfo.getColumnMapping();
            final Type[] types = mappingInfo.getColumnTypes();
            while (toCompare.next()) {
                if (reference.next()) {
                    if (!compareRowsByPrimitives(reference, toCompare, mapping, types)) {
//                if (!compareRowsByGetObject(reference, toCompare, mapping)) {
//                        LOG.info("Results have different values: row: " + position + ", column: " + i;
                        return false;
                    }

                } else {
//                LOG.info("Result have different sizes. Expected only " + position + " rows!");
                    return false;
                }
            }
            if (reference.next()) {
//            LOG.info("Result have different sizes. Expected more that " + position + " rows!");
                return false;
            }
//        LOG.info("Results are equal!");
            return true;
        } else {
            return false;
        }
    }
	
	private boolean compareRowsByPrimitives(final Row reference, final Row toCompare, final int[] mapping, final Type[] types) {
        for (int i = 0; i < mapping.length; ++i) {
            final boolean equalValues;
            final int mappedIndex = mapping[i];
            switch (types[i]) {
                case STRING:
                    final String testValue = toCompare.getString(i);
                    final String referenceValue = reference.getString(mappedIndex);
                    equalValues = testValue.equals(referenceValue);
                    break;
                case INTEGER:
                    final int refInt = reference.getInteger(mappedIndex);
                    equalValues = refInt == toCompare.getInteger(i);
                    break;
                case DOUBLE:
                    final double refDbl = reference.getDouble(mappedIndex);
                    equalValues = refDbl == toCompare.getDouble(i);
                    break;
                case DATE:
                    final Date testValue2 = toCompare.getDate(i);
                    final Date referenceValue2 = reference.getDate(mappedIndex);
                    equalValues = testValue2.equals(referenceValue2);
                    break;
                case BOOLEAN:
                    final boolean refBool = reference.getBoolean(mappedIndex);
                    equalValues = refBool == toCompare.getBoolean(i);
                    break;
                default:
                    final Object testValue3 = toCompare.getObject(i);
                    final Object referenceValue3 = reference.getObject(mappedIndex);
                    equalValues = testValue3.equals(referenceValue3);
                    break;
            }
//            if (!equalValues || reference.isNull(mappedIndex) != toCompare.isNull(i)) {
            if (!equalValues) {
                return false;
            }
        }
        return true;
    }
	
	private MappingInfo getFieldMappingFromMetaData(final RowCursor reference, final RowCursor toCompare, final boolean considerTableNames) throws Exception {
        final RowMetaData referenceMetaData = reference.getMetaData();
        final RowMetaData toCompareMetaData = toCompare.getMetaData();
        assertTrue("reference Row Cursor should return RowMetaData", referenceMetaData != null);
        assertTrue("toCompare Row Cursor should return RowMetaData", toCompareMetaData != null);
        final int refColumnCount = referenceMetaData.getColumnCount();
        final int toTestColCount = toCompareMetaData.getColumnCount();
        if (refColumnCount == toTestColCount) {
            final Map<ColumnInfo, Integer> toComparemapping = new HashMap<ColumnInfo, Integer>();
            final int[] columnMapping = new int[refColumnCount];
            final Type[] types = new Type[refColumnCount];
            for (int i = 0; i < refColumnCount; ++i) {
                final ColumnMetaData columnMetaData = toCompareMetaData.getColumnMetaData(i);
                final String toTestColumnName = columnMetaData.getName();
                final Table srcTable = columnMetaData.getSourceTable();
                final String toTestTableName;
                if (srcTable != null && considerTableNames) {
                    toTestTableName = columnMetaData.getSourceTable().getTableMetaData().getName();
                } else {
                    toTestTableName = "";
                }
                final Type toTestType = columnMetaData.getType();
                types[i] = toTestType;
                final ColumnInfo colInf = new ColumnInfo(toTestColumnName, toTestTableName, toTestType);
                toComparemapping.put(colInf, i);
            }
            for (int i = 0; i < refColumnCount; ++i) {
                final ColumnMetaData columnMetaData = referenceMetaData.getColumnMetaData(i);
                final String refColumnName = columnMetaData.getName();
                final String refTableName;
                final Table srcTable = columnMetaData.getSourceTable();
                if (srcTable != null && considerTableNames) {
                    refTableName = columnMetaData.getSourceTable().getTableMetaData().getName();
                } else {
                    refTableName = "";
                }
                final Type refType = columnMetaData.getType();
                final ColumnInfo colInf = new ColumnInfo(refColumnName, refTableName, refType);
                final Integer positionInToCompare = toComparemapping.get(colInf);
                assertTrue("ResultSets have different Schemas! No entry found for: " + colInf + ". Existing columns: " + toComparemapping, positionInToCompare != null);
                columnMapping[positionInToCompare] = i;
            }
            return new MappingInfo(columnMapping, types);
        } else {
            return null;
        }
    }
	
	 private void testForColumnCursor(ColumnCursor columnCursor) {
        while (columnCursor.next()) {
            final ColumnMetaData metaData = columnCursor.getMetaData();
            final Type type = metaData.getType();
            for (int i = 0; i < metaData.getRowCount(); ++i) {
                checkIndex(columnCursor, i, type);
            }
        }
    }
	 
	 private void testForRowCursor(RowCursor rowCursor) {
	        final RowMetaData metaData = rowCursor.getMetaData();
	        final Type[] rowTypes = new Type[metaData.getColumnCount()];
	        for (int i = 0; i < metaData.getColumnCount(); ++i) {
	            rowTypes[i] = metaData.getColumnMetaData(i).getType();
	        }
	        while (rowCursor.next()) {
	            for (int i = 0; i < metaData.getColumnCount(); ++i) {
	                checkIndex(rowCursor, i, rowTypes[i]);
	            }

	        }
	    }
	 
	 private void checkIndex(GenericLinearReadAccessible accessible, int index, Type type) {
        final boolean isNull = accessible.isNull(index);
        String getString = accessible.getString(index);
        Object getObject = accessible.getObject(index);
        if (isNull) {
            assertTrue("string should be null", getString == null);
            assertTrue("object should be null", getObject == null);
        }
        switch (type) {
            case INTEGER:
                int referenceInt = accessible.getInteger(index);
                if (isNull) {
                    assertEquals("should deliver Type.NULL_VALUE_INTEGER", null, referenceInt);

                } else {
                    assertEquals("conversion should be supported: int -> double", (double) referenceInt, accessible.getDouble(index));
                    assertEquals("conversion should be supported: int -> string", String.valueOf(referenceInt), getString);
                    assertEquals("conversion should be supported: int -> object", referenceInt, getObject);
                    try {
                        accessible.getBoolean(index);
                        fail("class cast exception expected: : int -> boolean");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDate(index);
                        fail("class cast exception expected: int -> date");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }

                break;
            case DOUBLE:
                double referenceDouble = accessible.getDouble(index);
                if (isNull) {
                    assertEquals("should deliver Type.NULL_VALUE_DOUBLE", null, referenceDouble);
                } else {
                    assertEquals("conversion should be supported: double -> int", (int) referenceDouble, accessible.getInteger(index));
                    assertEquals("conversion should be supported: double -> string", String.valueOf(referenceDouble), getString);
                    assertEquals("conversion should be supported: double -> object", referenceDouble, getObject);
                    try {
                        accessible.getBoolean(index);
                        fail("class cast exception expected: double -> boolean");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDate(index);
                        fail("class cast exception expected: double -> date");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }

                break;
            case BOOLEAN:
                boolean referenceBoolean = accessible.getBoolean(index);
                if (isNull) {
                    assertEquals("should deliver Type.NULL_VALUE_BOOLEAN", null, referenceBoolean);
                } else {
                    assertEquals("conversion should be supported: boolean -> string", String.valueOf(referenceBoolean), getString);
                    assertEquals("conversion should be supported: boolean -> object", referenceBoolean, getObject);
                    try {
                        accessible.getInteger(index);
                        fail("class cast exception expected: boolean -> int");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDouble(index);
                        fail("class cast exception expected: boolean -> double");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDate(index);
                        fail("class cast exception expected: boolean -> date");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }
                break;
            case DATE:
                Date referenceDate = accessible.getDate(index);
                if (isNull) {
                    assertTrue("date should be null", referenceDate == null);
                } else {
                    assertEquals("conversion should be supported: date -> string", String.valueOf(referenceDate), getString);
                    assertEquals("conversion should be supported: date -> object", referenceDate, getObject);
                    try {
                        accessible.getInteger(index);
                        fail("class cast exception expected: date -> int");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDouble(index);
                        fail("class cast exception expected: date -> double");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getBoolean(index);
                        fail("class cast exception expected: date -> boolean");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }
                break;
            default:
                break;
        }
    }
	
	//PRIVATE CLASSES FOR TESTING
	
	static final class TableCreationResult {

	    private Table table;
	    private ArrayIntList generatedIds;
	    private List<ColumnCST> columns;
	    private RowCST[] rArray;
	    //
	    public TableCreationResult(Table table, ArrayIntList generatedIds, List<ColumnCST> columns) {
	        this.table = table;
	        this.generatedIds = generatedIds;
	        this.columns = columns;
	    }

	    public List<ColumnCST> getColumns() {
	        return columns;
	    }
	    
	    public void setColumns(List<ColumnCST> columns) {
	        this.columns = columns;
	    }

	    public ArrayIntList getGeneratedIds() {
	        return generatedIds;
	    }

	    public void setGeneratedIds(ArrayIntList generatedIds) {
	        this.generatedIds = generatedIds;
	    }
	    
	    public void setRows(RowCST[] rArray){
	    	this.rArray = rArray;
	    }
	    
	    public RowCursor getRowCursor(){
	    	return new RowCursorCST(rArray);
	    }
	    
	    public RowCursor getRowCursorTestDelete(){
	    	RowCST[] newArray = new RowCST[1];
	    	newArray[0] = rArray[1];
	    	return new RowCursorCST(newArray);
	    }
	    
	    public Table getTable() {
	        return table;
	    }
	    
	    //public 
	}
	
	static final class MappingInfo {

        private final int[] columnMapping;
        private final Type[] columnTypes;

        public MappingInfo(int[] columnMapping, Type[] types) {
            this.columnMapping = columnMapping;
            this.columnTypes = types;
        }

        public int[] getColumnMapping() {
            return columnMapping;
        }

        public Type[] getColumnTypes() {
            return columnTypes;
        }
    }
	
	static final class ColumnInfo {

        public ColumnInfo(String columnName, String tableName, Type columnType) {
            this.columnName = columnName;
            this.tableName = tableName;
            this.columnType = columnType;
        }
        final String columnName;
        final String tableName;
        final Type columnType;

        @Override
        public boolean equals(Object obj) {
            //generated
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ColumnInfo other = (ColumnInfo) obj;
            if ((this.columnName == null) ? (other.columnName != null) : !this.columnName.equals(other.columnName)) {
                return false;
            }
            if ((this.tableName == null) ? (other.tableName != null) : !this.tableName.equals(other.tableName)) {
                return false;
            }
            if (this.columnType != other.columnType) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            //generated
            int hash = 3;
            hash = 67 * hash + (this.columnName != null ? this.columnName.hashCode() : 0);
            hash = 67 * hash + (this.tableName != null ? this.tableName.hashCode() : 0);
            hash = 67 * hash + (this.columnType != null ? this.columnType.hashCode() : 0);
            return hash;
        }

        @Override
        public String toString() {
            return tableName + "." + columnName + "(" + columnType + ")";
        }
    }

	public static void main(String[] args) {
		/*Esta clase sera dada por el profesor para probar el entregable 1. La clase de dara antes de la entrega del proyecto, y 
		debera ser incluida en el mismo para ser ejecutada en la revision.*/
		
		System.out.println("IMPORTANTE: Cada test es independiente, es decir, si un test falla, la ejecucion del siguiente deberia ejecutarse independientemente. Por eso, al inicio de cada test"
				+ " se encuentra la instruccion: storage = new StorageLayerSMMDS(); para 'borrar'y 'reiniciar' la capa de almacenamiento.");
		System.out.println("****** Iniciando test ****** ");
		System.out.println("****** Iniciando tests para probar StorageLayer (acciones sobrre tablas) ****** ");
		StorageLayerTest test = new StorageLayerTest();
		
		//Test 1
		test.testTableNameDuplication();
		//Test2
		test.testRenameTable();
		//Test 3
		try {
			test.testCreateTable();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Test 4
		try {
			test.testDeleteTable();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Test 5
		try {
			test.testGetTables();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("****** Iniciando tests para probar Table (acciones sobre columnas y filas de una tabla) ****** ");
		//Test 6
		try {
			test.testRenameColumn();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Test 7
		try {
			test.testCreateColumn();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Test 8
		try {
			test.testAddAndGetRows();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Test 9
		try {
			test.testDeleteRows();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Test 10
		try {
			test.testDropColumns();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("****** Finalizando tests para probar Table (acciones sobre columnas y filas de una tabla) ****** ");
		System.out.println("****** Iniciando tests para probar Cast y excepciones al leer los datos de una Columna o Fila  ****** ");
		//test 11
		try {
			test.testCastsColumn();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Test 12
		try {
			test.testCastsRow();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("****** Finalizando tests para probar Cast y excepciones al leer los datos de una Columna o Fila  ****** ");
		//Imprimir cantidad de errores
		test.cantidadErrores();
	}
}
