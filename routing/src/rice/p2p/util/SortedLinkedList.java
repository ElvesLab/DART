/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute 
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software 
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or 
promote products derived from this software without specific prior written 
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is" 
basis, without any representations or warranties of any kind, express or implied 
including, but not limited to, representations or warranties of 
non-infringement, merchantability or fitness for a particular purpose. In no 
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect, 
incidental, special, exemplary, or consequential damages (including, but not 
limited to, procurement of substitute goods or services; loss of use, data, or 
profits; or business interruption) however caused and on any theory of 
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if 
advised of the possibility of such damage.

*******************************************************************************/ 
package rice.p2p.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.ListIterator;

public class SortedLinkedList<E extends Comparable<E>> extends LinkedList<E> {

  @Override
  public boolean addAll(Collection<? extends E> c) {
    for (E elt : c) {
      add(elt);
    }
    return true;
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    throw new UnsupportedOperationException("Does not guarantee sortedness.");
  }

  @Override
  public void addFirst(E o) {
    throw new UnsupportedOperationException("Does not guarantee sortedness.");
  }

  @Override
  public void addLast(E o) {
    throw new UnsupportedOperationException("Does not guarantee sortedness.");
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    // TODO Auto-generated method stub
    final ListIterator<E> it = super.listIterator(index);
    
    return new ListIterator<E>(){

      public void add(E o) {
        throw new UnsupportedOperationException("Does not guarantee sortedness.");
      }

      public boolean hasNext() {
        return it.hasNext();
      }

      public boolean hasPrevious() {
        return it.hasPrevious();
      }

      public E next() {
        return it.next();
      }

      public int nextIndex() {
        return it.nextIndex();
      }

      public E previous() {
        return it.previous();
      }

      public int previousIndex() {
        return it.previousIndex();
      }

      public void remove() {
        it.remove();
      }

      public void set(E o) {
        throw new UnsupportedOperationException("Does not guarantee sortedness.");
      }    
    };
  }

  @Override
  public E set(int index, E element) {
    throw new UnsupportedOperationException("Does not guarantee sortedness.");
  }

  @Override
  public boolean add(E o) {
    // shortcuts
    if (isEmpty()) {
      super.add(o);
      return true;
    }
    
    if (getFirst().compareTo(o) >= 0) {
      super.addFirst(o);
      return true;
    }
    
    if (getLast().compareTo(o) <= 0) {
      super.addLast(o);
      return true;
    }
    
    ListIterator<E> i = super.listIterator(0);
    E elt;
    while(i.hasNext()) {
      elt = i.next(); 
      int diff = elt.compareTo(o);
      if (diff >= 0) break;
    }
    i.previous();
    i.add(o);
    return true;
  }

}
