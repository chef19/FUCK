/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.storage.impl;

import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.structures.DataStructure;
import dbs_project.structures.LinearDataStructure;
import java.util.Date;
import org.apache.commons.collections.primitives.IntIterator;

/**
 *
 * @author Kevin Matamoros
 */
public class FilaCursor implements RowCursor{
    public Fila fila;
    public ListaEnlazada FilasEnlazadas;
    public ListaEnlazada FilasEnlazadasFinal;
    public ListaDobleEnlazada FilDobleEnlazada;
    public ListaDobleEnlazada FilDobleEnlazadaFinal;
    public Cola FilQueue;
    public Cola FilQueueFinal;
    public Pila FilStack;
    public Pila FilStackFinal;
    
    
    public FilaCursor(ListaEnlazada Filas){
        this.fila=null;
        FilDobleEnlazada=null;
        FilQueue=null;
        FilStack=null;
        this.FilasEnlazadas=Filas;
        FilasEnlazadasFinal=new ListaEnlazada();        
    }
    public FilaCursor(ListaDobleEnlazada Filas){
        this.fila=null;
        this.FilDobleEnlazada=Filas;
        FilDobleEnlazadaFinal=new ListaDobleEnlazada();}
    public FilaCursor(Cola Filas){
        this.fila=null;
        this.FilQueue=Filas;
        FilQueueFinal=new Cola();}
    public FilaCursor(Pila Filas){
        this.fila=null;
        this.FilStack=Filas;
        FilStackFinal=new Pila();}
    
    @Override
    public RowMetaData getMetaData() {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.getMetaData();
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.getMetaData();
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.getMetaData();
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.getMetaData();
        }
    }

    @Override
    public Integer getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.getInteger(index);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.getInteger(index);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.getInteger(index);
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.getInteger(index);
        }
    }

    @Override
    public Boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.getBoolean(index);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.getBoolean(index);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.getBoolean(index);
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.getBoolean(index);
        }
    }

    @Override
    public Double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.getDouble(index);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.getDouble(index);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.getDouble(index);
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.getDouble(index);
        }
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.getDate(index);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.getDate(index);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.getDate(index);
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.getDate(index);
        }
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.getString(index);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.getString(index);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.getString(index);
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.getString(index);
        }
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.getObject(index);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.getObject(index);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.getObject(index);
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.getObject(index);
        }
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilasEnlazadas.current.getElemento();
            return fila.isNull(index);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.isNull(index);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilQueue.current.getElemento();
            return fila.isNull(index);
        }
        else{
            fila=(Fila) FilStack.top.getElemento();
            return fila.isNull(index);
        }
        
    }

    @Override
    public LinearDataStructure<?> asLinearDataStructure(DataStructure type) {
        if(FilasEnlazadas!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.asLinearDataStructure(type);
        }
        else if(FilDobleEnlazada!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.asLinearDataStructure(type);
        }
        else if(FilQueue!=null){
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.asLinearDataStructure(type);
        }
        else{
            fila=(Fila) FilDobleEnlazada.current.getElemento();
            return fila.asLinearDataStructure(type);
        }
    }

    @Override
    public boolean next() {
        if(FilasEnlazadas!=null){
            return FilasEnlazadas.next();
        }
        else if(FilDobleEnlazada!=null){
            return FilDobleEnlazada.next();
        }
        else if(FilQueue!=null){
            return FilQueue.dequeueBoolean();
        }
        else{
            return FilStack.NextVerdadero();
        }
    }

    @Override
    public int getCursorPosition() {
        if(FilasEnlazadas!=null){
            return FilasEnlazadas.getPosition();
        }
        else if(FilDobleEnlazada!=null){
            return FilDobleEnlazada.getPosition();
        }
        else if(FilQueue!=null){
            return FilQueue.posicion();
        }
        else{
            return FilStack.posicion();
        }
    }

    @Override
    public void close() {
        if(FilasEnlazadas!=null){
            FilasEnlazadasFinal.append(FilasEnlazadas.current.getElemento());
            FilasEnlazadas= FilasEnlazadasFinal;
        }
        else if(FilDobleEnlazada!=null){
            FilDobleEnlazadaFinal.append(FilDobleEnlazada.current.getElemento());
            FilDobleEnlazada= FilDobleEnlazadaFinal;
        }
        else if(FilQueue!=null){
            FilQueueFinal.enqueue(FilQueue.current.getElemento());
            FilQueue= FilQueueFinal;
        }
        else{
            FilStackFinal.push(FilStack.top.getElemento());
            FilStack= FilStackFinal;
        }   
    }

    @Override
    public DataStructure getType() {
        if(FilasEnlazadas!=null){
            return DataStructure.LINKEDLIST;
        }
        else if(FilDobleEnlazada!=null){
            return DataStructure.DOUBLYLINKEDLIST;
        }
        else if(FilQueue!=null){
            return DataStructure.QUEUE;
        }
        else{
            return DataStructure.STACK;
        }
    }
    
    
    public static void main(String[] args) {
        // TODO code application logic here
    ListaEnlazada Lista1= new ListaEnlazada();
    ListaEnlazada Lista2= new ListaEnlazada();
    ListaEnlazada Lista3= new ListaEnlazada();
    
    Lista1.append(1);
    Lista1.append(2);
    
    Lista2.append(3);
    Lista2.append(4);
    Lista2.append(5);
    Lista2.append(6);
    
    Lista3.append(6);
    
    Fila Fila1=new Fila(Lista1);
    Fila Fila2=new Fila(Lista2);
    Fila Fila3=new Fila(Lista3);
    
    ListaEnlazada Filas= new ListaEnlazada();
    Filas.append(Fila1);
    Filas.append(Fila2);
    Filas.append(Fila3);
    
    FilaCursor Cursor = new FilaCursor(Filas);
    
    
    System.out.println("Posicion: "+Cursor.getCursorPosition());
    System.out.println("Valor: "+Cursor.getInteger(1));
    System.out.println("Columnas de Fila: "+Cursor.getMetaData().getColumnCount());
    System.out.println("ID Fila: "+Cursor.getMetaData().getId());
    System.out.println("");
    
    Cursor.next();
    System.out.println("Posicion: "+Cursor.getCursorPosition());
    System.out.println("Valor: "+Cursor.getInteger(1));
    System.out.println("Columnas de Fila: "+Cursor.getMetaData().getColumnCount());
    System.out.println("ID Fila: "+Cursor.getMetaData().getId());
    System.out.println("");
    Cursor.close();
    
   System.out.println(Cursor.next());
    

    }
    
    
}
