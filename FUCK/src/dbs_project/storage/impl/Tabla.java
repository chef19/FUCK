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
    static ListaDobleEnlazada ColumnaDobleEnlazada;
    static ListaEnlazada Names;
    static ListaEnlazada MetaDatas;
    static Cola ColumnaQueue;
    static Pila ColumnaStack;
    CursorColumna ColumnaCursor;
    static ListaEnlazada Columnas;
    FilaCursor FilCursor;
    static int ID = 0;
    TableMetaData Datos;
    
    public Row Fila;
        
    
    public Tabla(String Name){
        Datos = new TablaMetaData(ID, Name);
        Columnas = new ListaEnlazada();
        FilEnlazada = new ListaEnlazada();
        FilDobleEnlazada= new ListaDobleEnlazada();
        FilQueue = new Cola();
        FilStack = new Pila();
        ID++;
    }
    
    public Tabla(String Name, int ID){
        Datos = new TablaMetaData(ID, Name);
        Columnas = new ListaEnlazada();
        FilEnlazada = new ListaEnlazada();
        FilDobleEnlazada= new ListaDobleEnlazada();
        FilQueue = new Cola();
        FilStack = new Pila();
    }
    @Override
    public void renameColumn(int columnId, String newColumnName) throws ColumnAlreadyExistsException, NoSuchColumnException {
        ColumnaCursor = new CursorColumna(Columnas);
        Columna Columna;
        int i=0;
        Columna temp = (Columna) ColumnaCursor.ColumnasEnlazadas.head.getElemento();
        if(temp.getMetaData().getId()==columnId){
            Columna = (Columna) ColumnaCursor.ColumnasEnlazadas.head.getElemento();
            Columna NuevaColumna = new Columna(Columna.Columna, newColumnName,
                        Columna.getMetaData().getType(),Columna.getMetaData().getId(),this);
            ColumnaCursor.ColumnasEnlazadas.head.setElemento(NuevaColumna);
            return;
        }
        else{
            while(i<ColumnaCursor.ColumnasEnlazadas.size()){
                ColumnaCursor.ColumnasEnlazadas.goToPos(i-1);
                Columna= (Columna) ColumnaCursor.ColumnasEnlazadas.current.getElemento();
                int compara=Columna.getMetaData().getId();
                if (compara==columnId){
                    Columna = (Columna) ColumnaCursor.ColumnasEnlazadas.current.getElemento();
                    Columna NuevaColumna = new Columna(Columna.Columna, newColumnName,
                                Columna.getMetaData().getType(),Columna.getMetaData().getId(),this);
                    ColumnaCursor.ColumnasEnlazadas.current.setElemento(NuevaColumna);
                    return;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchColumnException("Id no se encuentra en tabla");
    }

    @Override
    public int createColumn(String columnName, Type columnType) throws ColumnAlreadyExistsException {
        ListaEnlazada Lista = new ListaEnlazada();
        Columna Columna = new Columna(Lista, columnName, columnType, this);
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
        Columnas.append(column);
        return column.getMetaData().getId();
    }

    @Override
    public IntIterator addColumns(ColumnCursor columns) throws SchemaMismatchException, ColumnAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deleteRow(int rowID) throws NoSuchRowException {
        FilCursor = new FilaCursor(FilEnlazada);
        Fila Fils;
        int i=0;
        Fila temp = (Fila) FilCursor.FilasEnlazadas.head.getElemento();
        if(temp.getMetaData().getId()==rowID){
            FilCursor.FilasEnlazadas.remove();
            return;
        }
        else{
            while(i<FilCursor.FilasEnlazadas.size()){
                FilCursor.FilasEnlazadas.goToPos(i-1);
                Fils= (Fila) FilCursor.FilasEnlazadas.current.getElemento();
                int compara=Fils.getMetaData().getId();

                if (compara==rowID){
                    FilCursor.FilasEnlazadas.remove();
                    return;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchRowException("Id no se encuentra en tabla");
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
        ColumnaCursor = new CursorColumna(Columnas);
        Columna Columna;
        int i=0;
        Columna temp = (Columna) ColumnaCursor.ColumnasEnlazadas.head.getElemento();
        System.out.println(temp.getMetaData().getId());
        if(temp.getMetaData().getId()==columnId){
            ColumnaCursor.ColumnasEnlazadas.goToPos(-1);
            ColumnaCursor.ColumnasEnlazadas.remove();
            ColumnaCursor.ColumnasEnlazadas.next();
            return;
        }
        else{
            while(i<ColumnaCursor.ColumnasEnlazadas.size()){
                ColumnaCursor.ColumnasEnlazadas.goToPos(i-1);
                Columna= (Columna) ColumnaCursor.ColumnasEnlazadas.current.getElemento();
                int compara=Columna.getMetaData().getId();
                if (compara==columnId){
                    ColumnaCursor.ColumnasEnlazadas.remove();
                    return;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchColumnException("Id no se encuentra en tabla");
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
        Columna temp = (Columna) ColumnaCursor.ColumnasEnlazadas.head.getElemento();
        if(temp.getMetaData().getId()==columnId){
            return (Column) temp;
        }
        else{
            while(i<ColumnaCursor.ColumnasEnlazadas.size()){
                ColumnaCursor.ColumnasEnlazadas.goToPos(i-1);
                Columna= (Columna) ColumnaCursor.ColumnasEnlazadas.current.getElemento();
                int compara=Columna.getMetaData().getId();
                if (compara==columnId){
                    return Columna;
                }
                else{
                    i++;
                }
            }
            throw new NoSuchColumnException("Id no se encuentra en tabla");
        }
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
        Fila Fils;
        int i=0;
        Fila temp = (Fila) FilCursor.FilasEnlazadas.head.getElemento();
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
        }
        throw new NoSuchRowException("Id no se encuentra en tabla");
    }

    @Override
    public void updateRow(int rowID,Row newRow) throws SchemaMismatchException, NoSuchRowException {
        FilCursor = new FilaCursor(FilEnlazada);
        Fila Fila1;
        int i=0;
        Fila temp = (Fila) FilCursor.FilasEnlazadas.head.getElemento();
        if(temp.getMetaData().getId()==rowID){
            FilCursor.FilasEnlazadas.head.setElemento(newRow);
            return;
        }
        else{
            while(i<FilCursor.FilasEnlazadas.size()){
                FilCursor.FilasEnlazadas.goToPos(i-1);
                Fila1= (Fila) FilCursor.FilasEnlazadas.current.getElemento();
                int compara=Fila1.getMetaData().getId();
                if (compara==rowID){
                    Fila1 = (Fila) FilCursor.FilasEnlazadas.current.getElemento();
                    FilCursor.FilasEnlazadas.head.setElemento(newRow);
                    return;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchRowException("Id no se encuentra en tabla");
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
        Columna temp = (Columna) ColumnaCursor.ColumnasEnlazadas.head.getElemento();
        if(temp.getMetaData().getId()==columnId){
            ColumnaCursor.ColumnasEnlazadas.head.setElemento(updateColumn);
            return;
        }
        else{
            while(i<ColumnaCursor.ColumnasEnlazadas.size()){
                ColumnaCursor.ColumnasEnlazadas.goToPos(i-1);
                Columna= (Columna) ColumnaCursor.ColumnasEnlazadas.current.getElemento();
                int compara=Columna.getMetaData().getId();
                if (compara==columnId){
                    Columna = (Columna) ColumnaCursor.ColumnasEnlazadas.current.getElemento();
                    ColumnaCursor.ColumnasEnlazadas.head.setElemento(updateColumn);
                    return;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchColumnException("Id no se encuentra en tabla");
    }

    @Override
    public TableMetaData getTableMetaData() {
        return Datos;
    }

    @Override
    public RowCursor getRows(DataStructure type) {
        
        FilDobleEnlazada=new ListaDobleEnlazada();
        FilQueue = new Cola();
        FilStack = new Pila();
        
        if(type==DataStructure.DOUBLYLINKEDLIST){
            int i;
            
            for(i=0;i<FilEnlazada.size();i++){
                FilEnlazada.goToPos(i-1);
                FilDobleEnlazada.append(FilEnlazada.current);
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
            for(i=0;i<FilEnlazada.size();i++){
                FilEnlazada.goToPos(i-1);
                FilQueue.enqueue(FilEnlazada.current);
            }
            FilCursor =new FilaCursor(FilQueue);
            return FilCursor;
        }
        if(type==DataStructure.STACK){
            int i;
            for(i=0;i<FilEnlazada.size();i++){
                FilEnlazada.goToPos(i-1);
                FilStack.push(FilEnlazada.current);
            }
            FilCursor =new FilaCursor(FilStack);
            return FilCursor;
        }
        return null;
    }

    @Override
    public ColumnCursor getColumns(DataStructure type) {
        ColumnaDobleEnlazada=new ListaDobleEnlazada();
        ColumnaQueue = new Cola();
        ColumnaStack = new Pila();
        
        if(type==DataStructure.DOUBLYLINKEDLIST){
            int i;
            
            for(i=0;i<Columnas.size();i++){
                Columnas.goToPos(i-1);
                ColumnaDobleEnlazada.append(Columnas.current);
            }
            ColumnaCursor =new CursorColumna(ColumnaDobleEnlazada);
            return ColumnaCursor;
        }
        if(type==DataStructure.LINKEDLIST){
            ColumnaCursor =new CursorColumna(FilEnlazada);
            return ColumnaCursor;
        }
        if(type==DataStructure.QUEUE){
            int i;
            for(i=0;i<FilEnlazada.size();i++){
                FilEnlazada.goToPos(i-1);
                FilQueue.enqueue(FilEnlazada.current);
            }
            ColumnaCursor =new CursorColumna(FilQueue);
            return ColumnaCursor;
        }
        if(type==DataStructure.STACK){
            int i;
            for(i=0;i<FilEnlazada.size();i++){
                FilEnlazada.goToPos(i-1);
                FilStack.push(FilEnlazada.current);
            }
            ColumnaCursor =new CursorColumna(FilStack);
            return ColumnaCursor;
        }
        return null;
    }
    
    public static void main(String[] args) throws SchemaMismatchException, NoSuchRowException, ColumnAlreadyExistsException, NoSuchColumnException {
        // TODO code application logic here
    
        ListaEnlazada Lista1= new ListaEnlazada();
        ListaEnlazada Lista2= new ListaEnlazada();
        ListaEnlazada Lista3= new ListaEnlazada();
        ListaEnlazada Lista4= new ListaEnlazada();
        ListaEnlazada Lista5= new ListaEnlazada();
        ListaEnlazada ColumnasD= new ListaEnlazada();
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
        Tabla tabla=new Tabla("TB");
        //*********************
        Columna Columna1 =new Columna(Lista1, "1 a 3", Type.INTEGER, tabla);
        Columna Columna2 =new Columna(Lista2, "4 a 6", Type.INTEGER, tabla);
        Columna Columna3 =new Columna(Lista3, "7 a 9", Type.INTEGER, tabla);
        Columna Columna4 =new Columna(Lista4, "10 a 12", Type.INTEGER, tabla);
        Columna Columna5 =new Columna(Lista5, "13 a 15", Type.INTEGER, tabla);
        Columna Columna6 =new Columna(Lista5, "13 a 15", Type.INTEGER, tabla);
        Columna Columna7 =new Columna(Lista5, "13 a 15", Type.INTEGER, tabla);
        Columna Columna8 =new Columna(Lista5, "13 a 15", Type.INTEGER, tabla);

        ColumnasD.append(Columna1);// 0
        ColumnasD.append(Columna2);// 1
        ColumnasD.append(Columna3);// 2
        ColumnasD.append(Columna4);// 3
        ColumnasD.append(Columna5);// 4
        //*********************    
        System.out.println("Añade Columna4");           //3
        System.out.println(tabla.addColumn(Columna4));

        System.out.println("Añade Columna3");           //2
        System.out.println(tabla.addColumn(Columna3));

        System.out.println("Añade Columna1");           //0
        System.out.println(tabla.addColumn(Columna1));

        System.out.println("Añade Columna2");           //5
        System.out.println(tabla.addColumn(Columna6));

        System.out.println("Añade Columna5");           //6
        System.out.println(tabla.addColumn(Columna7));

        //*********************
        System.out.println("Cantidad de Columnas");
        System.out.println(Columnas.size());
        //*********************

        System.out.println("****************************************");
        CursorColumna Cursor = new CursorColumna(Columnas);
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
        /**
        System.out.println("Columna ID 3: "+tabla.getColumn(3).getMetaData().getName());
        tabla.renameColumn(3, "NuevoNombre");
        System.out.println("Columna ID 3: "+tabla.getColumn(3).getMetaData().getName());
        
        System.out.println("Cantidad de Columnas: "+Columnas.size());
        System.out.println("Columna ID 3: "+tabla.getColumn(3).getMetaData().getId());
        //System.out.println("Estructura de Datos de Cursor  "+Cursor.getType());
        * */

        System.out.println("*****************************************"); 
        System.out.println("*****************************************");
        System.out.println("Buscando ID 0 "+tabla.getColumn(0).getMetaData().getId());
        
        tabla.dropColumn(0);
        
        System.out.println("Eliminar ID 0");
        System.out.println("Buscando ID 0 "+tabla.getColumn(0).getMetaData().getId());
        
        /**
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
        Lista5.append(11);
        Lista5.append(12);
        Lista5.append(13);
        //*********************
        Tabla tabla=new Tabla("Tabla 1");
        //*********************
        Fila Fila1 =new Fila(Lista1);
        Fila Fila2 =new Fila(Lista2);
        Fila Fila3 =new Fila(Lista3);
        Fila Fila4 =new Fila(Lista4);
        Fila Fila5 =new Fila(Lista5,3);

        FilasD.append(Fila1);// 0
        FilasD.append(Fila2);// 1
        FilasD.append(Fila3);// 2
        FilasD.append(Fila4);// 3
        //*********************    
        System.out.println("Añade Fila4");           //3
        System.out.println(tabla.addRow(Fila4));

        System.out.println("Añade Fila3");           //2
        System.out.println(tabla.addRow(Fila3));

        System.out.println("Añade Fila1");           //0
        System.out.println(tabla.addRow(Fila1));

        System.out.println("Añade Fila2");           //1
        System.out.println(tabla.addRow(Fila2));


        //*********************
        System.out.println("Cantidad de Filas");
        System.out.println(FilEnlazada.size());
        //*********************

        /**System.out.println("****************************************");
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
        
        System.out.println("Fila ID 3: "+tabla.getRow(3).getInteger(0));      
        tabla.updateRow(3, Fila5);
        System.out.println("Fila ID 3: "+tabla.getRow(3).getInteger(0));
        //System.out.println("Estructura de Datos de Cursor  "+Cursor.getType());
        FilEnlazada.goToStart();
        Fila temp22 = (Fila) FilEnlazada.current.getElemento();
        System.out.println("Imprime"+temp22.getMetaData().getId());
        
        System.out.println("*****************************************"); 
        System.out.println("*****************************************");
        
        System.out.println(tabla.getRows(DataStructure.DOUBLYLINKEDLIST).getType());
        System.out.println(tabla.getRows(DataStructure.LINKEDLIST).getType());
        System.out.println(tabla.getRows(DataStructure.QUEUE).getType());
        System.out.println(tabla.getRows(DataStructure.STACK).getType());
        //tabla.deleteRow(4);
        //System.out.println("Eliminar ID 4");
       // System.out.println("Buscando ID 4 "+tabla.getRow(4));
        

    //**********************/
    }
}
 