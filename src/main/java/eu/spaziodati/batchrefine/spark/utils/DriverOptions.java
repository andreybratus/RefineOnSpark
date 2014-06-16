package eu.spaziodati.batchrefine.spark.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class DriverOptions {

	@Option(name = "-h", aliases = { "--host" }, usage = "bind driver program to host adress ('spark.driver.host'). default: will try to guess", required = false)
	private String driverHost = null;

	@Option(name = "-p", aliases = { "--port" }, usage = "bind driver program to specified port ('spark.driver.port'). default: random", required = false)
	private String driverPort = null;

	@Option(name = "-n", aliases = { "--name" }, usage = "set driver program name. default: 'Refine-Spark CLI'", required = false)
	private String appName = "Refine-Spark CLI";
	@Option(name = "-v", aliases = { "--log" }, usage = "set verbosity level to INFO. default: 'WARN'", required = false)
	public boolean verbosity = false;

	@Option(name = "-m", usage = "set executor memory size default: 1g", required = false)
	String executorMemory = "1g";
	@Argument
	private List<String> fArguments = new ArrayList<String>();

	public String getDriverHost() {
		return driverHost;
	}

	public String getDriverPort() {
		return driverPort;
	}

	public String getAppName() {
		return appName;
	}

	public Level getVerbose() {
		return (verbosity) ? Level.INFO : Level.WARN;
	}

	public String getExecutorMemory(){
		return executorMemory;
	}
	
	public List<String> getArguments() {
		return fArguments;
	}

}