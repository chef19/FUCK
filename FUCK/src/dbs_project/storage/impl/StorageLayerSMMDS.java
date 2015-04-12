/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.storage.impl;

import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.storage.StorageLayer;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import dbs_project.storage.Type;
import dbs_project.structures.DataStructure;
import dbs_project.structures.LinearDataStructure;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author max
 */
public class StorageLayerSMMDS implements StorageLayer{
    static ListaEnlazada Tablas;
    Tabla Tabla;
    Map<String, Type> Mapa;
    
    public StorageLayerSMMDS(){
        Tablas = new ListaEnlazada();
        Mapa = new HashMap<String, Type>();
    }

    @Override
    public int createTable(String tableName, Map<String, Type> schema, DataStructure type) throws TableAlreadyExistsException {
        Tabla = new Tabla(tableName);
        Mapa = schema;
        return Tabla.getTableMetaData().getId();
    }

    @Override
    public void deleteTable(int tableID) throws NoSuchTableException {
        if(tableID>=Tablas.size()){
            throw new NoSuchTableException("Id no se encuentra en tabla");
        }
        else{
            Columna Columna;
            int i=0;
            Columna temp = (Columna) Tablas.head.getElemento();
            System.out.println("temp: "+temp.getMetaData().getId());
            if(temp.getMetaData().getId()==tableID){
                Tablas.remove();
            }
            else{
                while(i<Tablas.size()){
                    Tablas.goToPos(i-1);
                    Columna= (Columna) Tablas.current.getElemento();
                    int compara=Columna.getMetaData().getId();
                    if (compara==tableID){
                        Tablas.remove();
                    }
                    else{
                        i++;
                    }
                }
            }
        }
    }

    @Override
    public void renameTable(int tableID, String newName) throws TableAlreadyExistsException, NoSuchTableException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Table getTable(int tableID) throws NoSuchTableException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public LinearDataStructure<? extends Table> getTables(DataStructure type) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, TableMetaData> getDatabaseSchema() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
