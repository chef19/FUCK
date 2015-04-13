/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.gui.impl;

import dbs_project.storage.impl.ListaEnlazada;
import javax.swing.JOptionPane;
import javax.swing.JTable;

/**
 *
 * @author Kevin Matamoros
 */
public class guardaTabla <T> {
    public ListaEnlazada ListaEnlazada;
    public ListaEnlazada ListaColumnas;
    public JTable tabla;
    public guardaTabla(){
    }
    public void GuardarColumna(JTable tabla){
        ListaEnlazada = new ListaEnlazada();
        ListaColumnas = new ListaEnlazada();
        int numeroColumnas = tabla.getColumnCount();
        int i;
        int j;
            for(i=0;i<numeroColumnas;i++){
                ListaEnlazada = new ListaEnlazada();
                for(j=0;j<tabla.getRowCount();j++){
                ListaEnlazada.append(tabla.getValueAt(j, 0));
                }
                System.out.println("Elementos en la Lista Enlazada: "+ListaEnlazada.size());
                ListaEnlazada.clear();
                ListaColumnas.append(ListaEnlazada);
            }
            System.out.println("Elementos en la Lista Enlazada: "+ListaEnlazada.size());
            System.out.println("Elementos en la Lista de Columnas: "+ListaColumnas.size());
    }
    public int pruebas(JTable tabla){
        int i = tabla.getColumnCount();
        int j = tabla.getRowCount();
        System.out.println("Numero de filas: "+ j);
        return i;
    }
    
    //public <T> Elementos(ListaEnlazada Lista){
    //    return 
   // }
    
}
