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
 * Created on Apr 6, 2005
 */
package rice.environment;

import java.io.IOException;
import java.util.*;

import rice.Destructable;
import rice.environment.exception.ExceptionStrategy;
import rice.environment.exception.simple.SimpleExceptionStrategy;
import rice.environment.logging.*;
import rice.environment.logging.file.FileLogManager;
import rice.environment.logging.simple.SimpleLogManager;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.processing.Processor;
import rice.environment.processing.sim.SimProcessor;
import rice.environment.processing.simple.SimpleProcessor;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simple.SimpleTimeSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.pastry.Id;
import rice.selector.SelectorManager;


/**
 * Used to provide properties, timesource, loggers etc to the FreePastry
 * apps and components.
 * 
 * XXX: Plan is to place the environment inside a PastryNode.
 * 
 * @author Jeff Hoye
 */
public class Environment implements Destructable {
  public static final String[] defaultParamFileArray = {"freepastry"};
   
  private SelectorManager selectorManager;
  private Processor processor;
  private RandomSource randomSource;
  private TimeSource time;
  private LogManager logManager;
  private Parameters params;
  private Logger logger;
  private ExceptionStrategy exceptionStrategy;

  private HashSet<Destructable> destructables = new HashSet<Destructable>();
  
  /**
   * Constructor.  You can provide null values for all/any paramenters, which will result
   * in a default choice.  If you want different defaults, consider extending Environment
   * and providing your own chooseDefaults() method.
   * 
   * @param sm the SelectorManager.  Default: rice.selector.SelectorManager
   * @param rs the RandomSource.  Default: rice.environment.random.simple.SimpleRandomSource
   * @param time the TimeSource.  Default: rice.environment.time.simple.SimpleTimeSource
   * @param lm the LogManager.  Default: rice.environment.logging.simple.SimpleLogManager
   * @param props the Properties.  Default: empty properties
   */
  public Environment(SelectorManager sm, Processor proc, RandomSource rs, TimeSource time, LogManager lm, Parameters params, ExceptionStrategy strategy) {
    this.selectorManager = sm;    
    this.randomSource = rs;
    this.time = time; 
    this.logManager = lm;
    this.params = params;
    this.processor = proc;
    this.exceptionStrategy = strategy;
    
    if (params == null) {
      throw new IllegalArgumentException("params cannot be null"); 
    }
    
    // choose defaults for all non-specified parameters
    chooseDefaults();
    logger = this.logManager.getLogger(getClass(), null);
    
    
    this.selectorManager.setEnvironment(this);
    
    addDestructable(this.time);
    
//    addDestructable(this.selectorManager);
//    addDestructable(this.processor);
    
  }
  
  /**
   * Convienience for defaults.
   * 
   * @param paramFileName the file where parameters are saved
   * @throws IOException
   */
  public Environment(String[] orderedDefaultFiles, String paramFileName) {
    this(null,null,null,null,null,new SimpleParameters(orderedDefaultFiles,paramFileName), null);
  }
  
  public static Environment directEnvironment(int randomSeed) {
    SimpleRandomSource srs = new SimpleRandomSource(randomSeed, null);
    Environment env = directEnvironment(srs);
    srs.setLogManager(env.getLogManager());
    return env;
  }
  
  public static Environment directEnvironment() {
    return directEnvironment(null);
  }
  
  public static Environment directEnvironment(RandomSource rs) {
    Parameters params = new SimpleParameters(Environment.defaultParamFileArray,null);
    DirectTimeSource dts = new DirectTimeSource(params);
    LogManager lm = generateDefaultLogManager(dts,params);
    dts.setLogManager(lm);
    SelectorManager selector = generateDefaultSelectorManager(dts,lm,rs);
    dts.setSelectorManager(selector);
    Processor proc = new SimProcessor(selector);
    Environment ret = new Environment(selector,proc,rs,dts,lm,
        params, generateDefaultExceptionStrategy(lm));
    return ret;
  }
  
  public Environment(String paramFileName) {
    this(defaultParamFileArray,paramFileName);
  }

  /**
   * Convienience for defaults.  Has no parameter file to load/store.
   */
  public Environment() {
    this(null);
  }

  /**
   * Can be easily overridden by a subclass.
   */
  protected void chooseDefaults() {
    // choose defaults for all non-specified parameters
//    if (params == null) {      
//      params = new SimpleParameters("temp"); 
//    }    
    if (time == null) {
      time = generateDefaultTimeSource(); 
    }
    if (logManager == null) {
      logManager = generateDefaultLogManager(time, params);
    }
    if (randomSource == null) {
      randomSource = generateDefaultRandomSource(params,logManager);
    }    
    if (selectorManager == null) {      
      selectorManager = generateDefaultSelectorManager(time, logManager, randomSource); 
    }
    if (processor == null) {    
      if (params.contains("environment_use_sim_processor") &&
          params.getBoolean("environment_use_sim_processor")) {
        processor = new SimProcessor(selectorManager);
      } else {
        processor = generateDefaultProcessor(); 
      }
    }
    
    if (exceptionStrategy == null) {
      exceptionStrategy = generateDefaultExceptionStrategy(logManager); 
    }
  }

  public static ExceptionStrategy generateDefaultExceptionStrategy(LogManager manager) {
    return new SimpleExceptionStrategy(manager);
  }
  
  public static RandomSource generateDefaultRandomSource(Parameters params, LogManager logging) {
    RandomSource randomSource;
    if (params.getString("random_seed").equalsIgnoreCase("clock")) {
      randomSource = new SimpleRandomSource(logging);
    } else {
      randomSource = new SimpleRandomSource(params.getLong("random_seed"), logging);      
    }
      
    return randomSource;
  }
  
  public static TimeSource generateDefaultTimeSource() {
    return new SimpleTimeSource();
  }
  
  public static LogManager generateDefaultLogManager(TimeSource time, Parameters params) {
    if (params.getBoolean("environment_logToFile")) {
      return new FileLogManager(time, params); 
    }
    return new SimpleLogManager(time, params); 
  }
  
  public static SelectorManager generateDefaultSelectorManager(TimeSource time, LogManager logging, RandomSource randomSource) {
    return new SelectorManager("Default", time, logging, randomSource);
  }
  
  public static Processor generateDefaultProcessor() {
    return new SimpleProcessor("Default");
  }
  
  // Accessors
  public SelectorManager getSelectorManager() {
    return selectorManager; 
  }
  public Processor getProcessor() {
    return processor; 
  }
  public RandomSource getRandomSource() {
    return randomSource; 
  }
  public TimeSource getTimeSource() {
    return time; 
  }
  public LogManager getLogManager() {
    return logManager; 
  }
  public Parameters getParameters() {
    return params; 
  }
  
  /**
   * Tears down the environment.  Calls params.store(), selectorManager.destroy().
   *
   */
  public void destroy() {
    try {
      params.store();
    } catch (IOException ioe) {      
      if (logger.level <= Logger.WARNING) logger.logException("Error during shutdown",ioe); 
    }
    if (getSelectorManager().isSelectorThread()) {
      callDestroyOnDestructables();
    } else {
      getSelectorManager().invoke(new Runnable() {
        public void run() {
          callDestroyOnDestructables();
        }
      });
    }
  }
  
  private void callDestroyOnDestructables() {
    Iterator<Destructable> i = new ArrayList<Destructable>(destructables).iterator();
    while(i.hasNext()) {
      Destructable d = (Destructable)i.next();
      d.destroy();
    }
    selectorManager.destroy();
    processor.destroy();    
  }

  public void addDestructable(Destructable destructable) {
    if (destructable == null) {
      if (logger.level <= Logger.WARNING) logger.logException("addDestructable(null)", new Exception("Stack Trace"));
      return;
    }
    destructables.add(destructable);
    
  }
  
  public void removeDestructable(Destructable destructable) {
    if (destructable == null) {
      if (logger.level <= Logger.WARNING) logger.logException("addDestructable(null)", new Exception("Stack Trace"));
      return;
    }
    destructables.remove(destructable);
  }

  public ExceptionStrategy getExceptionStrategy() {
    return exceptionStrategy;
  }
  
  public ExceptionStrategy setExceptionStrategy(ExceptionStrategy newStrategy) {
    ExceptionStrategy ret = exceptionStrategy;
    exceptionStrategy = newStrategy;
    return ret;
  }
  public Environment cloneEnvironment(String prefix) {
    return cloneEnvironment(prefix, false, false);
  }
  
  public Environment cloneEnvironment(String prefix, boolean cloneSelector, boolean cloneProcessor) {
    // new logManager
    LogManager lman = cloneLogManager(prefix);

    TimeSource ts = cloneTimeSource(lman);
    
    // new random source
    RandomSource rand = cloneRandomSource(lman);
    
    // new selector
    SelectorManager sman = cloneSelectorManager(prefix, ts, rand, lman, cloneSelector);
    
    // new processor
    Processor proc = cloneProcessor(prefix, lman, cloneProcessor);
        
    // build the environment
    Environment ret = new Environment(sman, proc, rand, getTimeSource(), lman,
        getParameters(), getExceptionStrategy());
  
    // gain shared fate with the rootEnvironment
    addDestructable(ret);     
      
    return ret;
  }

  protected TimeSource cloneTimeSource(LogManager lman) {
    return getTimeSource();
  }
  
  protected LogManager cloneLogManager(String prefix) {
    LogManager lman = getLogManager();
    if (lman instanceof CloneableLogManager) {
      lman = ((CloneableLogManager) getLogManager()).clone(prefix);
    }
    return lman;
  }
  
  protected SelectorManager cloneSelectorManager(String prefix, TimeSource ts, RandomSource rs, LogManager lman, boolean cloneSelector) {
    SelectorManager sman = getSelectorManager();
    if (cloneSelector) {
      sman = new SelectorManager(prefix + " Selector",
          ts, lman, rs);
    }
    return sman;
  }
  
  protected Processor cloneProcessor(String prefix, LogManager lman, boolean cloneProcessor) {
    Processor proc = getProcessor();
    if (cloneProcessor) {
      proc = new SimpleProcessor(prefix + " Processor");
    }

    return proc;
  }
  
  protected RandomSource cloneRandomSource(LogManager lman) {
    long randSeed = getRandomSource().nextLong();
    return new SimpleRandomSource(randSeed, lman);    
  }
}

