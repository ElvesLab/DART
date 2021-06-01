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
package org.mpisws.p2p.transport.peerreview;

public interface PeerReviewConstants extends StatusConstants {

  public static final short EVT_SEND = 0; // Outgoing message (followed by SENDSIGN entry)
  public static final short EVT_RECV = 1; // Incoming message (followed by SIGN entry)
  public static final short EVT_SIGN = 2;                   /* Signature on incoming message */
  public static final short EVT_ACK = 3;                                  /* Acknowledgment */
  public static final short EVT_CHECKPOINT = 4;                 /* Checkpoint of the state machine */
  public static final short EVT_INIT = 5;                    /* State machine is (re)started */
  public static final short EVT_SENDSIGN = 6;                   /* Signature on outgoing message */
  public static final short EVT_VRF = 7;                         /* New Si value in the VRF */
  public static final short EVT_CHOOSE_Q = 8;                           /* Choose Q array in VRF */
  public static final short EVT_CHOOSE_RAND = 9;                        /* Choose randomness in VRF */
  public static final short EVT_MAX_RESERVED = EVT_CHOOSE_RAND;    /* User defined events start here */

  public static final short EVT_SOCKET_OPEN_INCOMING = 30; 
  public static final short EVT_SOCKET_OPEN_OUTGOING = 31; 
  public static final short EVT_SOCKET_OPENED_OUTGOING = 32; 
  public static final short EVT_SOCKET_EXCEPTION = 33; 
  public static final short EVT_SOCKET_CLOSE = 34; 
  public static final short EVT_SOCKET_SHUTDOWN_OUTPUT = 35; 
  public static final short EVT_SOCKET_CLOSED = 36; 
  public static final short EVT_SOCKET_CAN_READ = 37; 
  public static final short EVT_SOCKET_CAN_WRITE = 38; 
  public static final short EVT_SOCKET_CAN_RW = 39; 
  public static final short EVT_SOCKET_READ = 40; 
  public static final short EVT_SOCKET_WRITE = 41;   
  
  public static final short EVT_MIN_SOCKET_EVT = EVT_SOCKET_OPEN_INCOMING;
  public static final short EVT_MAX_SOCKET_EVT = EVT_SOCKET_SHUTDOWN_OUTPUT;
  
  public static final short EX_TYPE_IO = 1;
  public static final short EX_TYPE_ClosedChannel = 2;
  public static final short EX_TYPE_Unknown = 0;
 
  /* Message types used in PeerReview */

  public static final short MSG_USERDATA = 16;            /* Contains data the application has sent */
  public static final short MSG_ACK = 17;                       /* Acknowledges an USERDATA message */
  public static final short MSG_ACCUSATION = 18;            /* Contains evidence about a third node */
  public static final short MSG_CHALLENGE = 19;            /* Contains evidence about the recipient */
  public static final short MSG_RESPONSE = 20;                      /* Answers a previous CHALLENGE */
  public static final short MSG_AUTHPUSH = 21;        /* Sent to a witness; contains authenticators */
  public static final short MSG_AUTHREQ = 22;    /* Asks a witness to return a recent authenticator */
  public static final short MSG_AUTHRESP = 23;                    /* Responds to a previous AUTHREQ */
  public static final short MSG_USERDGRAM = 24;    /* Contains a datagram from the app (not logged) */

  /* Evidence types (challenges and proofs) */
  public static final byte CHAL_AUDIT = 1;    
  public static final byte CHAL_SEND = 2;
  public static final byte RESP_SEND = CHAL_SEND;
  public static final byte RESP_AUDIT = CHAL_AUDIT;
  public static final byte PROOF_INCONSISTENT = 3;
  public static final byte PROOF_NONCONFORMANT = 4;

  /* Constants for reporting the status of a remote node to the application */

  public static final long DEFAULT_AUTH_PUSH_INTERVAL_MILLIS = 5000;
  public static final long DEFAULT_CHECKPOINT_INTERVAL_MILLIS = 10000L;
  public static final long MAINTENANCE_INTERVAL_MILLIS = 10000;
  public static final long DEFAULT_TIME_TOLERANCE_MILLIS = 60000;

  public static final int TI_CHECKPOINT = 99;
  public static final int TI_MAINTENANCE = 6;
  public static final int TI_AUTH_PUSH = 7;
  public static final int TI_MAX_RESERVED = TI_AUTH_PUSH;
  public static final int TI_STATUS_INFO = 101;
  public static final int MAX_STATUS_INFO = 100;
  
  /* Flags for AUDIT challenges */

  public static final byte FLAG_INCLUDE_CHECKPOINT = 1;                /* Include a full checkpoint */
  public static final byte FLAG_FULL_MESSAGES_SENDER = 2; /* Don't hash outgoing messages to sender */
  public static final byte FLAG_FULL_MESSAGES_ALL = 4;          /* Don't hash any outgoing messages */

  /**
   * Enum for EvidenceTool
   */
  public static final int VALID = 1;
  public static final int INVALID = 2;
  public static final int CERT_MISSING = 3;

  public static final long DEFAULT_AUDIT_INTERVAL_MILLIS = 10000;
  public static final int PROGRESS_INTERVAL_MILLIS = 100;
  public static final int INVESTIGATION_INTERVAL_MILLIS = 250;
  public static final int DEFAULT_LOG_DOWNLOAD_TIMEOUT = 2000;
  public static final int MAX_WITNESSED_NODES = 110;
  public static final int MAX_ACTIVE_AUDITS = 500;
  public static final int MAX_ACTIVE_INVESTIGATIONS = 10;
  public static final int MAX_ENTRIES_BETWEEN_CHECKPOINTS = 100;
  public static final int AUTH_CACHE_INTERVAL = 500000;
  
  public static final int TI_START_AUDITS = 3;
  public static final int TI_MAKE_PROGRESS = 4;

  public static final int STATE_SEND_AUDIT = 1;
  public static final int STATE_WAIT_FOR_LOG = 2;

  public static final int NO_CERTIFICATE = -1;
  public static final int SIGNATURE_BAD = 0;
  public static final int SIGNATURE_OK = 1;

}
