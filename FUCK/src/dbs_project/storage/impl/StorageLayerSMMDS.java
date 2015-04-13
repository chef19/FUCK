/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.storage.impl;

import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.storage.Column;
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
        Tablas.append(Tabla);
        return Tabla.getTableMetaData().getId();
    }

    @Override
    public void deleteTable(int tableID) throws NoSuchTableException {
        Tablas.goToStart();
        Tabla Tabla;
        int i=0;
        Tabla temp = (Tabla) Tablas.head.getElemento();
        if(temp.getTableMetaData().getId()==tableID){
            Tablas.remove();
            return;
        }
        else{
            while(i<Tablas.size()){
                Tablas.goToPos(i-1);
                Tabla= (Tabla) Tablas.current.getElemento();
                int compara=Tabla.getTableMetaData().getId();
                if (compara==tableID){
                    Tablas.remove();
                    return;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchTableException("Id no se encuentra en tabla");
    }

    @Override
    public void renameTable(int tableID, String newName) throws TableAlreadyExistsException, NoSuchTableException {
        Tablas.goToStart();
        Tabla Tabla;
        int i=0;
        Tabla temp = (Tabla) Tablas.head.getElemento();
        if(temp.getTableMetaData().getId()==tableID){
            Tabla = (Tabla) Tablas.head.getElemento();
            Tabla NuevaTabla = new Tabla(newName, Tabla.getTableMetaData().getId());
            Tablas.head.setElemento(NuevaTabla);
            return;
        }
        else{
            while(i<Tablas.size()){
                Tablas.goToPos(i-1);
                Tabla= (Tabla) Tablas.current.getElemento();
                int compara=Tabla.getTableMetaData().getId();
                if (compara==tableID){
                    Tabla = (Tabla) Tablas.head.getElemento();
                    Tabla NuevaTabla = new Tabla(newName, Tabla.getTableMetaData().getId());
                    Tablas.head.setElemento(NuevaTabla);
                    return;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchTableException("Id no se encuentra en tabla");
    }

    @Override
    public Table getTable(int tableID) throws NoSuchTableException {
        Tablas.goToStart();
        Tabla Tabla;
        int i=0;
        Tabla temp = (Tabla) Tablas.head.getElemento();
        if(temp.getTableMetaData().getId()==tableID){
            return (Tabla) temp;
        }
        else{
            while(i<Tablas.size()){
                Tablas.goToPos(i-1);
                Tabla= (Tabla) Tablas.current.getElemento();
                int compara=Tabla.getTableMetaData().getId();
                if (compara==tableID){
                    return Tabla;
                }
                else{
                    i++;
                }
            }
        }
        throw new NoSuchTableException("Id no se encuentra en tabla");
    }

    @Override
    public LinearDataStructure<? extends Table> getTables(DataStructure type) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, TableMetaData> getDatabaseSchema() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    public static void main(String [] args) throws TableAlreadyExistsException, NoSuchTableException{
        StorageLayerSMMDS Prueba = new StorageLayerSMMDS();
        Map<String, Type> MapaPrueba = new HashMap<String, Type>();
        System.out.println(Prueba.createTable("Tabla Prueba 1", MapaPrueba, DataStructure.STACK));
        System.out.println(Prueba.createTable("Tabla Prueba 2", MapaPrueba, DataStructure.STACK));
        System.out.println(Prueba.createTable("Tabla Prueba 3", MapaPrueba, DataStructure.STACK));
        System.out.println(Prueba.createTable("Tabla Prueba 4", MapaPrueba, DataStructure.STACK));
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        Tabla tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        Prueba.Tablas.goToStart();
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        Prueba.deleteTable(1);
        System.out.println("se Borra el ID 1");
        Prueba.deleteTable(3);
        System.out.println("se Borra el ID 3");
        Prueba.Tablas.goToStart();
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        System.out.println(Prueba.createTable("Tabla Prueba 5", MapaPrueba, DataStructure.STACK));
        System.out.println("Se agregar el ID 4");
        System.out.println(Prueba.createTable("Tabla Prueba 6", MapaPrueba, DataStructure.STACK));
        System.out.println("Se agregar el ID 5");
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        Prueba.Tablas.goToStart();
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        System.out.println(Tablas.next());
        tb = (Tabla) Prueba.Tablas.getElement();
        System.out.println(tb.getTableMetaData().getId());
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        System.out.println("Se busca ID 4");
        System.out.println(Prueba.getTable(4).getTableMetaData().getId());
        System.out.println("Se busca ID 5");
        System.out.println(Prueba.getTable(5).getTableMetaData().getId());
        System.out.println("Se busca ID 0");
        System.out.println(Prueba.getTable(0).getTableMetaData().getId());
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        System.out.println("Se busca ID 4");
        System.out.println(Prueba.getTable(4).getTableMetaData().getName());
        Prueba.renameTable(4, "Nuevo Nombre de ID 4");
        System.out.println("Se busca ID 4");
        System.out.println(Prueba.getTable(4).getTableMetaData().getName());
        System.out.println("Se busca ID 5");
        System.out.println(Prueba.getTable(5).getTableMetaData().getName());
        System.out.println("Se busca ID 0");
        System.out.println(Prueba.getTable(0).getTableMetaData().getName());
        
        System.out.println("/***************************");
        System.out.println("/***************************");
        
        
    }
}
