/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.storage.impl;

import dbs_project.exceptions.ColumnAlreadyExistsException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchRowException;
import dbs_project.exceptions.SchemaMismatchException;
import dbs_project.storage.Column;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import dbs_project.storage.Type;
import dbs_project.structures.DataStructure;
import org.apache.commons.collections.primitives.IntIterator;

/**
 *
 * @author Kevin Matamoros
 */
public class Tabla implements Table{
    static ListaEnlazada FilEnlazada;
    static ListaDobleEnlazada FilDobleEnlazada;
    static Cola FilQueue;
    static Pila FilStack;
    CursorColumna ColumnaCursor;
    static ListaEnlazada Columnas;
    FilaCursor FilCursor;
    
    public Row Fila;
        
    
    public Tabla(){
        FilEnlazada = new ListaEnlazada();
        FilDobleEnlazada= new ListaDobleEnlazada();
        FilQueue = new Cola();
        FilStack = new Pila();
    }
    @Override
    public void renameColumn(int columnId, String newColumnName) throws ColumnAlreadyExistsException, NoSuchColumnException {
        
    }

    @Override
    public int createColumn(String columnName, Type columnType) throws ColumnAlreadyExistsException {
        ListaEnlazada Lista = new ListaEnlazada();
        Columna Columna = new Columna(Lista, columnName, columnType);
        return(Columna.Datos.getId());
    }

    @Override
    public int addRow(Row row) throws SchemaMismatchException {
        FilEnlazada.append(row);
        return row.getMetaData().getId();
    }

    @Override
    public IntIterator addRows(RowCursor rows) throws SchemaMismatchException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int addColumn(Column column) throws SchemaMismatchException, ColumnAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public IntIterator addColumns(ColumnCursor columns) throws SchemaMismatchException, ColumnAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deleteRow(int rowID) throws NoSuchRowException {
        FilCursor = new FilaCursor(FilEnlazada);

            if(rowID>=FilCursor.FilasEnlazadas.size()){
                throw new NoSuchRowException("Id no se encuentra en tabla");
            }
            else{
                Fila Fils;
                int i=0;
                Fila temp = (Fila) FilCursor.FilasEnlazadas.head.getElemento();
                System.out.println("temp: "+temp.getMetaData().getId());
                if(temp.getMetaData().getId()==rowID){
                    FilCursor.FilasEnlazadas.remove();
                }
                else{
                        while(i<FilCursor.FilasEnlazadas.size()){
                            FilCursor.FilasEnlazadas.goToPos(i-1);
                            Fils= (Fila) FilCursor.FilasEnlazadas.current.getElemento();
                            int compara=Fils.getMetaData().getId();

                            if (compara==rowID){
                                FilCursor.FilasEnlazadas.remove();
                            }
                            else{
                                i++;
                            }
                        }
                }
            }
    }

    @Override
    public void deleteRows(IntIterator rowIDs) throws NoSuchRowException {
        while(rowIDs.hasNext()==true){
            FilEnlazada.remove();
            rowIDs.next();
        }
    }

    @Override
    public void dropColumn(int columnId) throws NoSuchColumnException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void dropColumns(IntIterator columnIds) throws NoSuchColumnException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Column getColumn(int columnId) throws NoSuchColumnException {
        ColumnaCursor = new CursorColumna(Columnas);
        Columna Columna;
        int i=0;
        while(i<=ColumnaCursor.Columnas.size()){
            Columna= (Columna) ColumnaCursor.Columnas.current.getElemento();
            if (Columna.getMetaData().getId()==columnId){
                return Columna;
            }
            else{
                ColumnaCursor.next();
                i++;
            }
        }
        return null;
    }

    @Override
    public ColumnCursor getColumns(DataStructure type, IntIterator columnIds) throws NoSuchColumnException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public RowCursor getRows(DataStructure type, IntIterator rowIds) throws NoSuchRowException {
        FilCursor = new FilaCursor(FilEnlazada);
        return FilCursor;
    }

    @Override
    public Row getRow(int rowId) throws NoSuchRowException {
      
            FilCursor = new FilaCursor(FilEnlazada);

            if(rowId>=FilCursor.FilasEnlazadas.size()){
                throw new NoSuchRowException("Id no se encuentra en tabla");
            }
            else{
                Fila Fils;
                int i=0;
                Fila temp = (Fila) FilCursor.FilasEnlazadas.head.getElemento();
                System.out.println("temp: "+temp.getMetaData().getId());
                if(temp.getMetaData().getId()==rowId){
                    return (Row) temp;
                }
                else{
                        while(i<FilCursor.FilasEnlazadas.size()){
                            FilCursor.FilasEnlazadas.goToPos(i-1);
                            Fils= (Fila) FilCursor.FilasEnlazadas.current.getElemento();
                            int compara=Fils.getMetaData().getId();

                            if (compara==rowId){
                                return Fils;
                            }
                            else{
                                i++;
                            }
                        }
                    return null;
                }
            }
        
    }

    @Override
    public void updateRow(int rowID,Row newRow) throws SchemaMismatchException, NoSuchRowException {
        FilEnlazada.goToPos(rowID);
        
    }
    @Override
    public void updateRows(IntIterator rowIDs, RowCursor newRows) throws SchemaMismatchException, NoSuchRowException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void updateColumns(IntIterator columnIDs, ColumnCursor updateColumns) throws SchemaMismatchException, NoSuchColumnException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void updateColumn(int columnId, Column updateColumn) throws SchemaMismatchException, NoSuchColumnException {
        ColumnaCursor = new CursorColumna(Columnas);
        Columna Columna;
        int i=0;
        while(i<=ColumnaCursor.Columnas.size()){
            Columna= (Columna) ColumnaCursor.Columnas.current.getElemento();
            if (Columna.getMetaData().getId()==columnId){
                ColumnaCursor.Columnas.current.setElemento(updateColumn);
                return;
            }
            else{
                ColumnaCursor.next();
                i++;
            }
        }
    }

    @Override
    public TableMetaData getTableMetaData() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public RowCursor getRows(DataStructure type) {
        if(type==DataStructure.DOUBLYLINKEDLIST){
            int i;
            for(i=0;i<FilEnlazada.size();i--){
                FilDobleEnlazada.append(FilEnlazada.current);
                FilEnlazada.remove();
            }
            FilCursor =new FilaCursor(FilDobleEnlazada);
            return FilCursor;
        }
        if(type==DataStructure.LINKEDLIST){
            FilCursor =new FilaCursor(FilEnlazada);
            return FilCursor;
        }
        if(type==DataStructure.QUEUE){
            int i;
            for(i=0;i<FilEnlazada.size();i--){
                FilQueue.enqueue(FilEnlazada.current);
                FilEnlazada.remove();
            }
        }
        if(type==DataStructure.STACK){
            int i;
            for(i=0;i<FilEnlazada.size();i--){
                FilEnlazada.goToPos(i);
                Fila temp =(Fila) FilEnlazada.current.getElemento();
                System.out.println(temp.getMetaData().getId());
                FilStack.push(FilEnlazada.current.getElemento());
            }
            System.out.println("SAAALIOOOOOOOOOOO");
            FilCursor =new FilaCursor(FilStack);
            return FilCursor;
        }
        return null;
    }

    @Override
    public ColumnCursor getColumns(DataStructure type) {
        if(type==DataStructure.DOUBLYLINKEDLIST){
            return null;
        }
        if(type==DataStructure.LINKEDLIST){
            return null;
        }
        if(type==DataStructure.QUEUE){
            return null;
        }
        if(type==DataStructure.STACK){
            return null;
        }
        return null;
    }
    
    public static void main(String[] args) throws SchemaMismatchException, NoSuchRowException {
        // TODO code application logic here
    ListaEnlazada Lista1= new ListaEnlazada();
    ListaEnlazada Lista2= new ListaEnlazada();
    ListaEnlazada Lista3= new ListaEnlazada();
    ListaEnlazada Lista4= new ListaEnlazada();
    ListaEnlazada Lista5= new ListaEnlazada();
    ListaEnlazada FilasD= new ListaEnlazada();
    //*********************
    Lista1.append(1);
    Lista1.append(2);
    //*********************
    Lista2.append(3);
    Lista2.append(4);
    Lista2.append(5);
    Lista2.append(6);
    //*********************
    Lista3.append(7);
    //*********************
    Lista4.append(8);
    Lista4.append(9);
    Lista4.append(10);
    
    Lista5.append(10);
    //*********************
    Tabla tabla=new Tabla();
    //*********************
    Fila Fila1 =new Fila(Lista1);
    Fila Fila2 =new Fila(Lista2);
    Fila Fila3 =new Fila(Lista3);
    Fila Fila4 =new Fila(Lista4);
    Fila Fila5 =new Fila(Lista5);
    
    FilasD.append(Fila1);// 0
    FilasD.append(Fila2);// 1
    FilasD.append(Fila3);// 2
    FilasD.append(Fila4);// 3
    FilasD.append(Fila5);// 4
    //*********************    
    System.out.println("Añade Fila4");           //3
    System.out.println(tabla.addRow(Fila4));
    
    System.out.println("Añade Fila3");           //2
    System.out.println(tabla.addRow(Fila3));
    
    System.out.println("Añade Fila1");           //0
    System.out.println(tabla.addRow(Fila1));
    
    
    
    System.out.println("Añade Fila2");           //1
    System.out.println(tabla.addRow(Fila2));
    
    System.out.println("Añade Fila5");           //4
    System.out.println(tabla.addRow(Fila5));
    
    //*********************
    System.out.println("Cantidad de Filas");
    System.out.println(FilEnlazada.size());
    //*********************
    
    System.out.println("****************************************");
    FilaCursor Cursor = new FilaCursor(FilEnlazada);
    Cursor.next();
    System.out.println("****************************************");
    System.out.println("Posicion: "+Cursor.getCursorPosition());
    System.out.println("ID: "+ Cursor.getMetaData().getId());
    Cursor.next();
    
    System.out.println("Posicion: "+Cursor.getCursorPosition());
    System.out.println("ID: "+Cursor.getMetaData().getId()); 

    Cursor.next();
    System.out.println("Posicion: "+Cursor.getCursorPosition());
    System.out.println("ID: "+Cursor.getMetaData().getId()); 

    Cursor.next();
    System.out.println("Posicion: "+Cursor.getCursorPosition());
    System.out.println("ID: "+Cursor.getMetaData().getId()); 
    
    Cursor.next();
    System.out.println("Posicion: "+Cursor.getCursorPosition());
    System.out.println("ID: "+Cursor.getMetaData().getId()); 
    
    System.out.println("*****************************************"); 
    System.out.println("*****************************************"); 

    System.out.println("Fila ID 5: "+tabla.getRow(5).getMetaData().getId());
    //System.out.println("Estructura de Datos de Cursor  "+Cursor.getType());
    
    System.out.println("*****************************************"); 
    System.out.println("*****************************************");
    //tabla.deleteRow(2);
    //System.out.println("Eliminar ID 3");
    //System.out.println("Buscando ID 2 "+tabla.getRow(2));
    

    //**********************/
    }
}
 