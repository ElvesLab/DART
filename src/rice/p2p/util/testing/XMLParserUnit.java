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
package rice.p2p.util.testing;

import rice.p2p.util.*;
import java.io.*;
import java.util.*;

public class XMLParserUnit {
  
  public static void main(String[] argv) throws Exception {
    System.out.println("XMLParser Test Suite");
    System.out.println("-------------------------------------------------------------");
    System.out.println("  Running Tests");
    
    System.out.print("    Testing Simple (1)\t\t\t\t");
    
    XMLParser parser = new XMLParser();
    parser.setInput(new StringReader("<test></test>"));
    int i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if (i == XMLParser.END_DOCUMENT) {
          System.out.println("[ PASSED ]");
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(5):\t" + i);
        }
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getName());
      }
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    System.out.print("    Testing Simple (2)\t\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test/>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();

      if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if (i == XMLParser.END_DOCUMENT) {
          System.out.println("[ PASSED ]");
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i);
        } 
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getName());
      }
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    System.out.print("    Testing Simple Attribute (1)\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=bar/>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if (i == XMLParser.END_DOCUMENT) {
          System.out.println("[ PASSED ]");
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i);
        }
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getName());
      }
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    System.out.print("    Testing Simple Attribute (2)\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo='bar'/>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if (i == XMLParser.END_DOCUMENT) {
          System.out.println("[ PASSED ]");
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i);
        }    
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getName());
      }
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    System.out.print("    Testing Simple Attribute (3)\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=\"bar\"/>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if (i == XMLParser.END_DOCUMENT) {
          System.out.println("[ PASSED ]");
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i);
        }      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getName());
      }
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    System.out.print("    Testing Simple Attribute (4)\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=\"bar\"></test>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if (i == XMLParser.END_DOCUMENT) {
          System.out.println("[ PASSED ]");
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i);
        }      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getName());
      }
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    System.out.print("    Testing Simple Attribute (5)\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=\"bar\" baz=blah goo=29.33   ></test>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if (i == XMLParser.END_DOCUMENT) {
          System.out.println("[ PASSED ]");
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i);
        }      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getName());
      }
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    
    System.out.print("    Testing Simple Attribute (6)\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=\"bar\" baz=blah goo=29.33   ></test>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      
      if (parser.getAttributeValue(null, "foo").equals("bar") && 
          parser.getAttributeValue(null, "baz").equals("blah") && 
          parser.getAttributeValue(null, "goo").equals("29.33")) {
        i = parser.next();
        
        if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
          i = parser.next();
          
          if (i == XMLParser.END_DOCUMENT) {
            System.out.println("[ PASSED ]");
          } else {
            System.out.println("[ FAILED ]");
            System.out.println("    Output(4):\t" + i);
          }      
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i + " " + parser.getName());
        }
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getAttributeValue(null, "foo").equals("bar") + " " + 
                           parser.getAttributeValue(null, "baz").equals("blah") + " " + 
                           parser.getAttributeValue(null, "goo").equals("29.33"));
      }  
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }    
    
    System.out.print("    Testing Missing Attribute\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=\"bar\" baz=blah goo=29.33   ></test>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      
      if (parser.getAttributeValue(null, "foo").equals("bar") && 
          parser.getAttributeValue(null, "baz").equals("blah") && 
          parser.getAttributeValue(null, "goo").equals("29.33") &&
          parser.getAttributeValue(null, "bar") == null) {
        i = parser.next();
        
        if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
          i = parser.next();
          
          if (i == XMLParser.END_DOCUMENT) {
            System.out.println("[ PASSED ]");
          } else {
            System.out.println("[ FAILED ]");
            System.out.println("    Output(4):\t" + i);
          }      
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(3):\t" + i + " " + parser.getName());
        }
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(2):\t" + i + " " + parser.getAttributeValue(null, "foo").equals("bar") + " " + 
                           parser.getAttributeValue(null, "baz").equals("blah") + " " + 
                           parser.getAttributeValue(null, "goo").equals("29.33"));
      }  
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }    

    System.out.print("    Testing Recursive\t\t\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=\"bar\" baz=blah goo=29.33   >\n\t<bar/>\t\t\t\n\t</test>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.TEXT) && (parser.getText().equals("\n\t")) && (parser.isWhitespace())) {
        i = parser.next();
        
        if ((i == XMLParser.START_TAG) && (parser.getName().equals("bar"))) {
          i = parser.next();
          
          if ((i == XMLParser.END_TAG) && (parser.getName().equals("bar"))) {
            i = parser.next();
            
            if ((i == XMLParser.TEXT) && (parser.getText().equals("\t\t\t\n\t")) && (parser.isWhitespace())) {
              i = parser.next();
              
              if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
                i = parser.next();
                
                if (i == XMLParser.END_DOCUMENT) {
                  System.out.println("[ PASSED ]");
                } else {
                  System.out.println("[ FAILED ]");
                  System.out.println("    Output(5):\t" + i);
                }
              } else {
                System.out.println("[ FAILED ]");
                System.out.println("    Output(4):\t" + i + " " + parser.getName());
              }
            } else {
              System.out.println("[ FAILED ]");
              System.out.println("    Output(3t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
            }    
          } else {
            System.out.println("[ FAILED ]");
            System.out.println("    Output(3):\t" + i + " " + parser.getName());
          }
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(2):\t" + i + " " + parser.getName());
        }
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(1t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
      }      
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    
    System.out.print("    Testing Nasty\t\t\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<test foo=\"bar\" baz=   6 goo=  \t29.33   >\n\t<bar   lah\n=\n\n\ndofdo/>\t\t\t\n\t</test>"));
    i = parser.next();
    
    if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
      i = parser.next();
      
      if ((i == XMLParser.TEXT) && (parser.getText().equals("\n\t")) && (parser.isWhitespace())) {
        i = parser.next();
        
        if ((i == XMLParser.START_TAG) && (parser.getName().equals("bar"))) {
          i = parser.next();
          
          if ((i == XMLParser.END_TAG) && (parser.getName().equals("bar"))) {
            i = parser.next();
            
            if ((i == XMLParser.TEXT) && (parser.getText().equals("\t\t\t\n\t")) && (parser.isWhitespace())) {
              i = parser.next();
              
              if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
                i = parser.next();
                
                if (i == XMLParser.END_DOCUMENT) {
                  System.out.println("[ PASSED ]");
                } else {
                  System.out.println("[ FAILED ]");
                  System.out.println("    Output(5):\t" + i);
                }
              } else {
                System.out.println("[ FAILED ]");
                System.out.println("    Output(4):\t" + i + " " + parser.getName());
              }
            } else {
              System.out.println("[ FAILED ]");
              System.out.println("    Output(3t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
            }    
          } else {
            System.out.println("[ FAILED ]");
            System.out.println("    Output(3):\t" + i + " " + parser.getName());
          }
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(2):\t" + i + " " + parser.getName());
        }
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(1t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
      }      
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(1):\t" + i + " " + parser.getName());
    }
    
    System.out.print("    Testing Start Document\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<?xml version='1.0'?><test foo=\"bar\" baz=   6 goo=  \t29.33   >\n\t<bar   lah\n=\n\n\ndofdo/>\t\t\t\n\t</test>"));
    i = parser.next();
    
/*    if (i == XMLParser.START_DOCUMENT) {
      i = parser.next();*/
      
      if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
        i = parser.next();
        
        if ((i == XMLParser.TEXT) && (parser.getText().equals("\n\t")) && (parser.isWhitespace())) {
          i = parser.next();
          
          if ((i == XMLParser.START_TAG) && (parser.getName().equals("bar"))) {
            i = parser.next();
            
            if ((i == XMLParser.END_TAG) && (parser.getName().equals("bar"))) {
              i = parser.next();
              
              if ((i == XMLParser.TEXT) && (parser.getText().equals("\t\t\t\n\t")) && (parser.isWhitespace())) {
                i = parser.next();
                
                if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
                  i = parser.next();
                  
                  if (i == XMLParser.END_DOCUMENT) {
                    System.out.println("[ PASSED ]");
                  } else {
                    System.out.println("[ FAILED ]");
                    System.out.println("    Output(5):\t" + i);
                  }
                } else {
                  System.out.println("[ FAILED ]");
                  System.out.println("    Output(4):\t" + i + " " + parser.getName());
                }
              } else {
                System.out.println("[ FAILED ]");
                System.out.println("    Output(3t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
              }    
            } else {
              System.out.println("[ FAILED ]");
              System.out.println("    Output(3):\t" + i + " " + parser.getName());
            }
          } else {
            System.out.println("[ FAILED ]");
            System.out.println("    Output(2):\t" + i + " " + parser.getName());
          }
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(1t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
        }      
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(1):\t" + i + " " + parser.getName());
      }
/*    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(0):\t" + i);
    }*/
    
    System.out.print("    Testing Nasty Start Document\t\t\t");
    
    parser = new XMLParser();
    parser.setInput(new StringReader("<?xml version='1.0' foo   = 'baz\n'  \t\t ?>\n\n \t<test foo=\"bar\" baz=   6 goo=  \t29.33   >\n\t<bar   lah\n=\n\n\ndofdo/>\t\t\t\n\t</test>"));
    i = parser.next();
    
/*    if (i == XMLParser.START_DOCUMENT) {
      i = parser.next(); */
      
      if ((i == XMLParser.TEXT) && (parser.getText().equals("\n\n \t")) && (parser.isWhitespace())) {
        i = parser.next();
        
        if ((i == XMLParser.START_TAG) && (parser.getName().equals("test"))) {
          i = parser.next();
          
          if ((i == XMLParser.TEXT) && (parser.getText().equals("\n\t")) && (parser.isWhitespace())) {
            i = parser.next();
            
            if ((i == XMLParser.START_TAG) && (parser.getName().equals("bar"))) {
              i = parser.next();
              
              if ((i == XMLParser.END_TAG) && (parser.getName().equals("bar"))) {
                i = parser.next();
                
                if ((i == XMLParser.TEXT) && (parser.getText().equals("\t\t\t\n\t")) && (parser.isWhitespace())) {
                  i = parser.next();
                  
                  if ((i == XMLParser.END_TAG) && (parser.getName().equals("test"))) {
                    i = parser.next();
                    
                    if (i == XMLParser.END_DOCUMENT) {
                      System.out.println("[ PASSED ]");
                    } else {
                      System.out.println("[ FAILED ]");
                      System.out.println("    Output(5):\t" + i);
                    }
                  } else {
                    System.out.println("[ FAILED ]");
                    System.out.println("    Output(4):\t" + i + " " + parser.getName());
                  }
                } else {
                  System.out.println("[ FAILED ]");
                  System.out.println("    Output(3t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
                }    
              } else {
                System.out.println("[ FAILED ]");
                System.out.println("    Output(3):\t" + i + " " + parser.getName());
              }
            } else {
              System.out.println("[ FAILED ]");
              System.out.println("    Output(2):\t" + i + " " + parser.getName());
            }
          } else {
            System.out.println("[ FAILED ]");
            System.out.println("    Output(1t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
          }      
        } else {
          System.out.println("[ FAILED ]");
          System.out.println("    Output(1):\t" + i + " " + parser.getName());
        }
      } else {
        System.out.println("[ FAILED ]");
        System.out.println("    Output(0t):\t" + i + " " + parser.getText() + " " + parser.isWhitespace());
      }    /*
    } else {
      System.out.println("[ FAILED ]");
      System.out.println("    Output(0):\t" + i);
    } */
    
    System.out.println("-------------------------------------------------------------");
  }

}