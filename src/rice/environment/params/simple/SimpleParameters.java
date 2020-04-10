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
package rice.environment.params.simple;

import java.io.*;
import java.net.*;
import java.util.*;

import rice.environment.params.ParameterChangeListener;
import rice.environment.params.Parameters;

/**
 * This class represents a generic Java process launching program which reads in
 * preferences from a preferences file and then invokes another JVM using those
 * prefs. If the launched JVM dies, this process can be configured to restart
 * the JVM any number of times before giving up. This process can also be
 * configured to launch the second JVM with a specified memory allocation,
 * etc...
 * 
 * @author Alan Mislove
 */
public class SimpleParameters implements Parameters {

  private MyProperties properties;

  private MyProperties defaults;

  private Set<ParameterChangeListener> changeListeners;

  /**
   * Where items are written out.
   */
  private String configFileName;

  public static final String FILENAME_EXTENSION = ".params";

  public static final String ARRAY_SPACER = ",";

  public static final String defaultParamsFile = "user"+FILENAME_EXTENSION;
  
  /**
   * 
   * @param orderedDefaults
   * @param mutableConfigFileName
   *          if this is null, no params are saved, if this file doesn't exist,
   *          you will get a warning printed to stdErr, then the file will be
   *          created if you ever store
   * @throws IOException
   */
  public SimpleParameters(String[] orderedDefaults, String mutableConfigFileName) {
    if (mutableConfigFileName != null) {
      this.configFileName = mutableConfigFileName + FILENAME_EXTENSION;
    } else {
      try {
        File f = new File(defaultParamsFile);
//        System.out.println(f.getAbsolutePath());
        if (f.exists()) {
          this.configFileName = defaultParamsFile; 
        }
      } catch (Exception e) {
        e.printStackTrace(); 
      }
    }
    this.properties = new MyProperties();
    this.defaults = new MyProperties();
    this.changeListeners = new HashSet<ParameterChangeListener>();

    for (int ctr = 0; ctr < orderedDefaults.length; ctr++) {
      try {        
        ClassLoader loader = this.getClass().getClassLoader();
        // some VMs report the bootstrap classloader via null-return
        if (loader == null)
          loader = ClassLoader.getSystemClassLoader();
        this.defaults.load(loader.getResource(
            orderedDefaults[ctr] + FILENAME_EXTENSION).openStream());
      } catch (Exception ioe) {
        String errorString = "Warning, couldn't load param file:"
          + (orderedDefaults[ctr] + FILENAME_EXTENSION);
//        System.err.println(errorString);
//        ioe.printStackTrace(System.err);
        throw new ParamsNotPresentException(errorString,ioe);
      }
    }

    if (this.configFileName != null) {
      File f = new File(this.configFileName);
      if (f.exists()) {
        try {
          properties.load(new FileInputStream(this.configFileName));
        } catch (Exception e) {
          throw new ParamsNotPresentException("Error loading "+f, e);
        }
      } else {
        System.err.println("Configuration file " + f.getAbsolutePath()
            + " not present.  Using defaults.");
      }
    }
  }

  @SuppressWarnings("unchecked")
  public Enumeration enumerateDefaults() {
    return defaults.keys();
  }

  @SuppressWarnings("unchecked")
  public Enumeration enumerateNonDefaults() {
    return properties.keys();
  }

  protected InetSocketAddress parseInetSocketAddress(String name)
      throws UnknownHostException {
    String host = name.substring(0, name.indexOf(":"));
    String port = name.substring(name.indexOf(":") + 1);

    try {
      return new InetSocketAddress(InetAddress.getByName(host), Integer
          .parseInt(port));
    } catch (UnknownHostException uhe) {
      System.err.println("ERROR: Unable to find IP for ISA " + name
          + " - returning null.");
      return null;
    }
  }

  protected String getProperty(String name) {
    String result = properties.getProperty(name);

    if (result == null)
      result = defaults.getProperty(name);

    if (result == null) {
      System.err.println("WARNING: The parameter '" + name
          + "' was not found - this is likely going to cause an error.");
      //You " +
      //                     "can fix this by adding this parameter (and appropriate value) to the
      // proxy.params file in your ePOST " +
      //                     "directory.");
    } else {
      // remove any surrounding whitespace
      result = result.trim();
    }

    return result;
  }

  /**
   * Note, this method does not implicitly call store()
   * 
   * @see #store()
   * @param name
   * @param value
   */
  protected void setProperty(String name, String value) {
    if ((defaults.getProperty(name) != null)
        && (defaults.getProperty(name).equals(value))) {
      // setting property back to default, remove override property if any
      if (properties.getProperty(name) != null) {
        properties.remove(name);
        fireChangeEvent(name, value);
      }
    } else {
      if ((properties.getProperty(name) == null)
          || (!properties.getProperty(name).equals(value))) {
        properties.setProperty(name, value);
        fireChangeEvent(name, value);
      }
    }
  }

  public void remove(String name) {
    properties.remove(name);
    fireChangeEvent(name, null);
  }

  public boolean contains(String name) {
    if (defaults.containsKey(name))
      return true;
    return properties.containsKey(name);
  }

  public int getInt(String name) {
    try {
      return Integer.parseInt(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  public double getDouble(String name) {
    try {
      return Double.parseDouble(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  public float getFloat(String name) {
    try {
      return Float.parseFloat(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  public long getLong(String name) {
    try {
      return Long.parseLong(getProperty(name));
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(nfe.getMessage() + " for parameter "
          + name);
    }
  }

  public boolean getBoolean(String name) {
    return (new Boolean(getProperty(name))).booleanValue();
  }

  public InetAddress getInetAddress(String name) throws UnknownHostException {
    return InetAddress.getByName(getString(name));
  }

  public InetSocketAddress getInetSocketAddress(String name)
      throws UnknownHostException {
    return parseInetSocketAddress(getString(name));
  }

  public InetSocketAddress[] getInetSocketAddressArray(String name)
      throws UnknownHostException {
    if (getString(name).length() == 0)
      return new InetSocketAddress[0];
      
    String[] addresses = getString(name).split(ARRAY_SPACER);
    List<InetSocketAddress> result = new LinkedList<InetSocketAddress>();

    for (int i = 0; i < addresses.length; i++) {
      InetSocketAddress address = parseInetSocketAddress(addresses[i]);

      if (address != null)
        result.add(address);
    }

    return (InetSocketAddress[]) result.toArray(new InetSocketAddress[0]);
  }

  public String getString(String name) {
    return getProperty(name);
  }

  public String[] getStringArray(String name) {
    String list = getProperty(name);

    if (list != null)
      return (list.equals("") ? new String[0] : list.split(ARRAY_SPACER));
    else
      return null;
  }

  public void setInt(String name, int value) {
    setProperty(name, Integer.toString(value));
  }

  public void setDouble(String name, double value) {
    setProperty(name, Double.toString(value));
  }

  public void setFloat(String name, float value) {
    setProperty(name, Float.toString(value));
  }

  public void setLong(String name, long value) {
    setProperty(name, Long.toString(value));
  }

  public void setBoolean(String name, boolean value) {
    setProperty(name, "" + value);
  }

  public void setInetAddress(String name, InetAddress value) {
    setProperty(name, value.getHostAddress());
  }

  public void setInetSocketAddress(String name, InetSocketAddress value) {
    setProperty(name, value.getAddress().getHostAddress() + ":"
        + value.getPort());
  }

  public void setInetSocketAddressArray(String name, InetSocketAddress[] value) {
    StringBuffer buffer = new StringBuffer();

    for (int i = 0; i < value.length; i++) {
      buffer.append(value[i].getAddress().getHostAddress() + ":"
          + value[i].getPort());
      if (i < value.length - 1)
        buffer.append(ARRAY_SPACER);
    }

    setProperty(name, buffer.toString());
  }

  public void setString(String name, String value) {
    setProperty(name, value);
  }

  public void setStringArray(String name, String[] value) {
    StringBuffer buffer = new StringBuffer();

    for (int i = 0; i < value.length; i++) {
      buffer.append(value[i]);
      if (i < value.length - 1)
        buffer.append(ARRAY_SPACER);
    }

    setProperty(name, buffer.toString());
  }

  public void store() throws IOException {
    if (configFileName == null)
      return;
    try {
      File current = new File(configFileName);
      File next = new File(configFileName + ".new");
      File old = new File(configFileName + ".old");

      properties.store(new FileOutputStream(next), null);
      
      current.renameTo(old);
      next.renameTo(current);
      old.delete();
    } catch (IOException ioe) {
      System.err.println("[Loader       ]: Unable to store properties file "
          + configFileName + ", got error " + ioe);
      throw ioe;
    }
  }

  public void addChangeListener(ParameterChangeListener p) {
    changeListeners.add(p);
  }

  public void removeChangeListener(ParameterChangeListener p) {
    changeListeners.remove(p);
  }

  private void fireChangeEvent(String name, String val) {
    Iterator<ParameterChangeListener> i = changeListeners.iterator();
    while (i.hasNext()) {
      ParameterChangeListener p = (ParameterChangeListener) i.next();
      p.parameterChange(name, val);
    }
  }

  protected class MyProperties extends Properties {
    @SuppressWarnings("unchecked")
    public Enumeration keys() {
      final String[] keys = (String[]) keySet().toArray(new String[0]);
      Arrays.sort(keys);

      return new Enumeration() {
        int pos = 0;

        public boolean hasMoreElements() {
          return (pos < keys.length);
        }

        public Object nextElement() {
          return keys[pos++];
        }
      };
    }
  }
}
