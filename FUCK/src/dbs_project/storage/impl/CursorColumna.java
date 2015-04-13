/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.storage.impl;

import dbs_project.storage.ColumnCursor;
import dbs_project.storage.impl.Columna;
import dbs_project.storage.ColumnMetaData;
import dbs_project.structures.DataStructure;
import dbs_project.structures.LinearDataStructure;
import java.util.Date;

/**
 *
 * @author max
 */
public class CursorColumna implements ColumnCursor{

    public Columna Columna;
    public ListaEnlazada ColumnasEnlazadas;
    public ListaEnlazada ColumnasEnlazadasFinal;
    public ListaDobleEnlazada ColumnasDobleEnlazada;
    public ListaDobleEnlazada ColumnasDobleEnlazadaFinal;
    public Cola ColumnasQueue;
    public Cola ColumnasQueueFinal;
    public Pila ColumnasStack;
    public Pila ColumnasStackFinal;
    
    public CursorColumna(ListaEnlazada Columnas){
        this.Columna=null;
        ColumnasDobleEnlazada=null;
        ColumnasQueue=null;
        ColumnasStack=null;
        this.ColumnasEnlazadas=Columnas;
        ColumnasEnlazadasFinal=new ListaEnlazada();
    }
    public CursorColumna(ListaDobleEnlazada Columnas){
        this.Columna=null;
        this.ColumnasDobleEnlazada=Columnas;
        ColumnasDobleEnlazadaFinal=new ListaDobleEnlazada();
    }
    public CursorColumna(Cola Columnas){
        this.Columna=null;
        this.ColumnasQueue=Columnas;
        ColumnasQueueFinal=new Cola();
    }
    public CursorColumna(Pila Columnas){
        this.Columna=null;
        this.ColumnasStack=Columnas;
        ColumnasStackFinal=new Pila();
    }
    
    
    @Override
    public ColumnMetaData getMetaData() {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.getMetaData();
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.getMetaData();
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.getMetaData();
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.getMetaData();
        }
    }

    @Override
    public Integer getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.getInteger(index);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.getInteger(index);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.getInteger(index);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.getInteger(index);
        }
    }

    @Override
    public Boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.getBoolean(index);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.getBoolean(index);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.getBoolean(index);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.getBoolean(index);
        }
    }

    @Override
    public Double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.getDouble(index);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.getDouble(index);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.getDouble(index);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.getDouble(index);
        }
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.getDate(index);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.getDate(index);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.getDate(index);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.getDate(index);
        }
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.getString(index);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.getString(index);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.getString(index);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.getString(index);
        }
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.getObject(index);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.getObject(index);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.getObject(index);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.getObject(index);
        }
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.isNull(index);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.isNull(index);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.isNull(index);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.isNull(index);
        }
    }

    @Override
    public LinearDataStructure<?> asLinearDataStructure(DataStructure type) {
        if(ColumnasEnlazadas!=null){
            Columna=(Columna) ColumnasEnlazadas.current.getElemento();
            return Columna.asLinearDataStructure(type);
        }
        else if(ColumnasDobleEnlazada!=null){
            Columna=(Columna) ColumnasDobleEnlazada.current.getElemento();
            return Columna.asLinearDataStructure(type);
        }
        else if(ColumnasQueue!=null){
            Columna=(Columna) ColumnasQueue.current.getElemento();
            return Columna.asLinearDataStructure(type);
        }
        else{
            Columna=(Columna) ColumnasStack.top.getElemento();
            return Columna.asLinearDataStructure(type);
        }
    }

    @Override
    public boolean next() {
        if(ColumnasEnlazadas!=null){
            return ColumnasEnlazadas.next();
        }
        else if(ColumnasDobleEnlazada!=null){
            return ColumnasDobleEnlazada.next();
        }
        else if(ColumnasQueue!=null){
            return ColumnasQueue.dequeueBoolean();
        }
        else{
            return ColumnasStack.NextVerdadero();
        }
    }

    @Override
    public int getCursorPosition() {
        if(ColumnasEnlazadas!=null){
            return ColumnasEnlazadas.getPosition();
        }
        else if(ColumnasDobleEnlazada!=null){
            return ColumnasDobleEnlazada.getPosition();
        }
        else if(ColumnasQueue!=null){
            return ColumnasQueue.posicion();
        }
        else{
            return ColumnasStack.posicion();
        }
    }

    @Override
    public void close() {
        if(ColumnasEnlazadas!=null){
            ColumnasEnlazadasFinal.append(ColumnasEnlazadas.current.getElemento());
            ColumnasEnlazadas= ColumnasEnlazadasFinal;
        }
        else if(ColumnasDobleEnlazada!=null){
            ColumnasDobleEnlazadaFinal.append(ColumnasDobleEnlazada.current.getElemento());
            ColumnasDobleEnlazada= ColumnasDobleEnlazadaFinal;
        }
        else if(ColumnasQueue!=null){
            ColumnasQueueFinal.enqueue(ColumnasQueue.current.getElemento());
            ColumnasQueue= ColumnasQueueFinal;
        }
        else{
            ColumnasStackFinal.push(ColumnasStack.top.getElemento());
            ColumnasStack= ColumnasStackFinal;
        }
    }

    @Override
    public DataStructure getType() {
        if(ColumnasEnlazadas!=null){
            return DataStructure.LINKEDLIST;
        }
        else if(ColumnasDobleEnlazada!=null){
            return DataStructure.DOUBLYLINKEDLIST;
        }
        else if(ColumnasQueue!=null){
            return DataStructure.QUEUE;
        }
        else{
            return DataStructure.STACK;
        }
    }
    
}
