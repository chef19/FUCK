/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.storage.impl;

import dbs_project.structures.DataStructure;
import dbs_project.structures.LinearList;

/**
 *
 * @author max
 */
public class ListaEnlazada <T> implements LinearList<T>{

    public Nodo head;
    public Nodo current;
    public Nodo tail;
    public int position;
    public int size;

    public ListaEnlazada() {
        this.head = new Nodo();
        this.current = this.head;
        this.tail = this.head;
        this.size = 0;
        this.position = -1;
    }

    @Override
    public void insert(T element) {
        Nodo newNode = new Nodo(element, this.current.getNext());
        this.current.setNext(newNode);
        if (this.current == this.tail) {
            this.tail = tail.getNext();
        }
        this.size++;

    }

    @Override
    public void append(T element) {
        Nodo newNode = new Nodo(element);
        this.tail.setNext(newNode);
        this.tail = newNode;
        this.size++;
    }

    @Override
    public void remove() {
        if ((this.head == this.current) && (this.head == this.tail)){
            System.out.println("Lista vacía, no se puede remover ningún elemento");
            return;
        }
        Nodo temp = head;
        while (temp.getNext() != this.current) {
            temp = temp.getNext();
        }
        temp.setNext(this.current.getNext());
        if (this.current == this.tail) {
            this.current = this.tail = temp;
            this.position--;
        }

        else
            this.current = this.current.getNext();
        this.size--;
    }

    @Override
    public void clear() {
        this.head = this.tail = this.current = new Nodo();
        this.position = -1;
        this.size = 0;
    }

    @Override
    public int getPosition() {
        return this.position;
    }

    @Override
    public void goToStart(){
        this.current = this.head;
        this.position = -1;
    }

    @Override
    public void goToEnd(){
        this.current = this.tail;
        this.position = this.size - 1;
    }

    @Override
    public void goToPos(int pos) {
        if (pos < 0 || pos >= this.size) {
            System.out.println("Posición inválida");
            return;
        }
        if (pos > this.position) {
            while (pos > this.position) {
                this.next();
            }
        } else if (pos < this.position) {
            while (pos < this.position) {
                this.previous();
            }
        }
    }

    @Override
    public int getPositionOfElement(T element) {
        Nodo tempNode = this.head;
        int position = -1;
        while (tempNode != null) {
            if (tempNode.getElemento() != null && tempNode.getElemento().equals(element)){
                return position;
            }
            tempNode = tempNode.getNext();
            position++;
        }
        return -1;
    }

    @Override
    public DataStructure getType() {
        return DataStructure.LINKEDLIST;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        if (size == 0){
            return true;
        }
        return false;
    }

    @Override
    public boolean next() {
        if (this.current == this.tail) {
            System.out.println("Actualmente en último nodo, no se puede avanzar");
            return false;
        }
        this.current = this.current.getNext();
        this.position++;    
        return true;
    }

    @Override
    public boolean previous() {
        if (this.current == this.head) {
            System.out.println("Actualmente en primer nodo, no se puede retroceder");
            return false;
        }
        Nodo temp = head;
        this.position = -1;
        while (temp.getNext() != this.current) {
            temp = temp.getNext();
            this.position++;
        }
        this.current = temp;
        return true;
    }

    @Override
    public T getElement() {
        return (T) current.getElemento();    
    }
    
    public static void main(String [] args){
        ListaEnlazada Lista = new ListaEnlazada();
        Lista.append(1);
        Lista.append(2);
        Lista.append(3);
        Lista.append(4);
        //*************************
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.previous());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.previous());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.next());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println("/************************");
        Lista.goToPos(1);
        System.out.println(Lista.getElement());
        Lista.goToPos(3);
        System.out.println(Lista.getElement());
        Lista.goToPos(0);
        System.out.println(Lista.getElement());
        Lista.goToPos(3);
        System.out.println(Lista.getElement());
        System.out.println("/************************");
        Lista.insert(5);
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        Lista.insert(6);
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println("/************************");
        Lista.goToStart();
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        Lista.goToPos(1);
        Lista.remove();
        Lista.goToStart();
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        System.out.println(Lista.getElement());
        System.out.println(Lista.next());
        
    }
}    
