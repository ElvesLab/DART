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
/*
 * Created on Jun 8, 2006
 */
package rice.pastry.socket.nat.sbbi;

import java.io.IOException;
import java.net.*;
import java.util.*;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.pastry.socket.nat.CantFindFirewallException;
import rice.pastry.socket.nat.NATHandler;

import net.sbbi.upnp.impls.InternetGatewayDevice;
import net.sbbi.upnp.messages.*;

public class SBBINatHandler implements NATHandler {

  Logger logger;
  Environment environment;
  
  boolean searchedForFireWall = false;

  InternetGatewayDevice fireWall;

  InetAddress fireWallExternalAddress;

  InetAddress localAddress;
  
  public SBBINatHandler(Environment env, InetAddress localAddress) {
    environment = env;
    logger = env.getLogManager().getLogger(SBBINatHandler.class, null); 
    this.localAddress = localAddress;
  }

  public synchronized InetAddress findFireWall(InetAddress bindAddress)
      throws IOException {
//    NetworkInterface ni = NetworkInterface.getByInetAddress(bindAddress);
    if (searchedForFireWall)
      return fireWallExternalAddress;
    searchedForFireWall = true;
    int discoveryTimeout = environment.getParameters().getInt(
        "nat_discovery_timeout");

    InternetGatewayDevice[] IGDs = InternetGatewayDevice
        .getDevices(discoveryTimeout);
    // use this code with the next version of sbbi's upnp library, it will only
    // search for the firewall on the given NetworkInterface
    // InternetGatewayDevice[] IGDs =
    // InternetGatewayDevice.getDevices(discoveryTimeout,
    // Discovery.DEFAULT_TTL,
    // Discovery.DEFAULT_MX,
    // ni);
    if (IGDs != null) {
      // no idea how to handle this if there are 2 firewalls... handle the first
      // one
      // if they have that interesting of a network, then they know what they
      // are doing
      // and can configure port forwarding
      fireWall = IGDs[0];
      try {
        fireWallExternalAddress = InetAddress.getByName(fireWall.getExternalIPAddress());
      } catch (UPNPResponseException ure) {
        if (logger.level <= Logger.WARNING) logger.logException("Error:",ure);
        throw new IOException(ure.toString()); 
      }
    } else {
      throw new CantFindFirewallException(
          "Could not find firewall for bindAddress:"+bindAddress);
    }
    return fireWallExternalAddress;
  }

  /**
   * Returns the external port to use based on querying the firewall for both
   * TCP and UDP
   * 
   * @param internal // the internal port we expect it to be mapped to
   * @param external // the first port to try, will search by incrementing
   * @return
   * @throws IOException
   * @throws UPNPResponseException
   */
  int findPortTries = 0;

  public int findAvailableFireWallPort(int internal, int external, int tries, String appName)
      throws IOException {
    try {
      findPortTries++;
      if (findPortTries > tries)
        throw new IOException("Couldn't find available port on firewall");
      if (logger.level <= Logger.FINE)
        logger.log("findFireWallPort(" + internal + "," + external + ")");
      ActionResponse response = null;
      response = fireWall.getSpecificPortMappingEntry(null, external, "TCP");
      if (checkSpecificPortMappingEntryResponse(response, internal, external,
          "TCP", appName)) {
        // continue
      } else {
        return findAvailableFireWallPort(internal, external + 1, tries, appName);
      }
  
      response = fireWall.getSpecificPortMappingEntry(null, external, "UDP");
      if (checkSpecificPortMappingEntryResponse(response, internal, external,
          "UDP", appName)) {
        // continue
      } else {
        return findAvailableFireWallPort(internal, external + 1, tries, appName);
      }
      return external;
    } catch (UPNPResponseException ure) {
      if (logger.level <= Logger.WARNING) logger.logException("Error:",ure);
      throw new IOException(ure.toString()); 
    }
  }

  public static final int MAX_PORT = 65535;

  /**
   * We return true if the response matches our app exactly, or if the entries
   * are invalid (because some firewalls do this instad of returning an error
   * that there is no entry.)
   * 
   * @param response
   * @param internal
   * @param external
   * @param type TCP or UDP
   * @return
   */
  @SuppressWarnings("unchecked")
  private boolean checkSpecificPortMappingEntryResponse(
      ActionResponse response, int internal, int external, String type,
      String app) {
    if (response == null)
      return true;

    if (logger.level <= Logger.FINEST) {
      Set s = response.getOutActionArgumentNames();
      Iterator i = s.iterator();
      while (i.hasNext()) {
        String key = (String) i.next();
        String val = response.getOutActionArgumentValue(key);
        System.out.println("  " + key + " -> " + val);
      }
    }

    boolean ret = true;
    // NewInternalPort->9001
    String internalPortStr = response
        .getOutActionArgumentValue("NewInternalPort");
    if (internalPortStr != null) {
      try {
        int internalPort = Integer.parseInt(internalPortStr);
        if (internalPort > MAX_PORT) {
          if (logger.level < Logger.WARNING)
            logger
                .log("Warning, NAT "
                    + fireWall.getIGDRootDevice().getModelName()
                    + " returned an invalid value for entry NewInternalPort.  Expected an integer less than "
                    + MAX_PORT + ", got: " + internalPort + ".  Query "
                    + external + ":" + type + "... overwriting entry.");
          return true; // a bogus entry from the firewall,
        }
        if (internalPort != internal) {
          if (logger.level <= Logger.FINER)
            logger.log("internalPort(" + internalPort + ") != internal("
                + internal + ")");
          ret = false; // but don't return yet, maybe it will become more clear
                        // with another clue
        }
      } catch (NumberFormatException nfe) {
        // firewall bug, assume we can overwrite
        if (logger.level < Logger.WARNING)
          logger
              .log("Warning, NAT "
                  + fireWall.getIGDRootDevice().getModelName()
                  + " returned an invalid value for entry NewInternalPort.  Expected an integer, got: "
                  + internalPortStr + ".  Query " + external + ":" + type
                  + "... overwriting entry.");
        return true;
      }
    }

    // NewPortMappingDescription->freepastry
    String appName = response
        .getOutActionArgumentValue("NewPortMappingDescription");
    if (appName != null) {
      // this is in case the app name is just a bunch of spaces
      String tempName = appName.replaceAll(" ", "");
      if ((tempName.equals("")) || appName.equalsIgnoreCase(app)) {
        // do nothing yet
      } else {
        if (logger.level <= Logger.FINER)
          logger.log("appName(" + appName + ") != app(" + app + ")");
        ret = false;
      }
    }

    // NewInternalClient->192.168.1.64
    String newInternalClientString = response
        .getOutActionArgumentValue("NewInternalClient");
    if (newInternalClientString == null) {
      if (logger.level < Logger.WARNING)
        logger
            .log("Warning, NAT "
                + fireWall.getIGDRootDevice().getModelName()
                + " returned no value for entry NewInternalClient.  Expected an IP address, got: "
                + newInternalClientString + ".  Query " + external + ":" + type
                + "... overwriting entry.");
      return true;
    }
    try {
      InetAddress client = InetAddress.getByName(newInternalClientString);
      if (!client.equals(localAddress)) {
        if (logger.level <= Logger.FINER)
          logger.log("client(" + client + ") != localAddress(" + localAddress
              + ")");
        ret = false;
      }
    } catch (Exception e) {
      if (logger.level < Logger.WARNING)
        logger
            .log("Warning, NAT "
                + fireWall.getIGDRootDevice().getModelName()
                + " returned an invalid value for entry NewInternalClient.  Expected an IP address, got: "
                + newInternalClientString + ".  Query " + external + ":" + type
                + "... overwriting entry.");
      return true;
    }

    // NewEnabled->1
    String enabledString = response.getOutActionArgumentValue("NewEnabled");
    if (enabledString != null) {
      try {
        int enabled = Integer.parseInt(enabledString);
        if (enabled == 0) {
          if (logger.level < Logger.FINE)
            logger
                .log("Warning, NAT "
                    + fireWall.getIGDRootDevice().getModelName()
                    + " had an existing rule that was disabled, implicitly overwriting.  "
                    + "Query " + external + ":" + type + "."
                    + "\n  NewInternalPort -> " + internalPortStr
                    + "\n  NewPortMappingDescription -> " + appName
                    + "\n  NewInternalClient -> " + newInternalClientString
                    + "\n  NewEnabled -> " + enabledString);
          return true; // the current rule is not used, go ahead and overwrite
        } else if (enabled == 1) {
          // nothing, depend on previous settings of ret to determine what to do
        } else {
          if (logger.level < Logger.WARNING)
            logger
                .log("Warning, NAT "
                    + fireWall.getIGDRootDevice().getModelName()
                    + " returned an invalid value for entry NewEnabled.  Expected 0 or 1, got: "
                    + enabled + ".  Query " + external + ":" + type
                    + "... overwriting entry.");
          return true; // a bogus entry from the firewall,
        }
      } catch (NumberFormatException nfe) {
        // firewall bug, assume we can overwrite
        if (logger.level < Logger.WARNING)
          logger
              .log("Warning, NAT "
                  + fireWall.getIGDRootDevice().getModelName()
                  + " returned an invalid value for entry NewEnabled.  Expected 0 or 1, got: "
                  + enabledString + ".  Query " + external + ":" + type
                  + "... overwriting entry.");
        return true;
      }
    } else {
      // router didn't specify enable string, no info, do nothing
    }

    if (ret == false) {
      if (logger.level < Logger.INFO)
        logger.log("Warning, NAT " + fireWall.getIGDRootDevice().getModelName()
            + " had an existing rule, trying different port.  " + "Query "
            + external + ":" + type + "." + "\n  NewInternalPort -> "
            + internalPortStr + "\n  NewPortMappingDescription -> " + appName
            + "\n  NewInternalClient -> " + newInternalClientString
            + "\n  NewEnabled -> " + enabledString);
    }
    return ret;
  }

  public void openFireWallPort(int local, int external, String appName) throws IOException {
    try {
      boolean mapped = true;
      mapped = fireWall.addPortMapping(appName, null, local, external, localAddress.getHostAddress(),
          0, "UDP");
      if (!mapped)
        throw new IOException(
            "Could not set firewall UDP port forwarding from external:"
                + fireWallExternalAddress + ":" + external + " -> local:"
                + localAddress + ":" + local);
      mapped = fireWall.addPortMapping(appName, null, local, external, localAddress.getHostAddress(),
          0, "TCP");
      if (!mapped)
        throw new IOException(
            "Could not set firewall TCP port forwarding from external:"
                + fireWallExternalAddress + ":" + external + " -> local:"
                + localAddress + ":" + local);
    } catch (UPNPResponseException ure) {
      if (logger.level <= Logger.WARNING) logger.logException("Error:",ure);
      throw new IOException(ure.toString()); 
    }
  }

  public InetAddress getFireWallExternalAddress() {
    return fireWallExternalAddress;
  }
}
