package dev.logic;

import java.util.Date;
import java.util.TimerTask;

import dev.logic.rules.ScaleOutRule;
import org.easyrules.api.RulesEngine;
import org.easyrules.core.RulesEngineBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.client.ClientEmulator;
import dev.logic.rules.ScaleInRule;

public class Controller extends TimerTask {
	private ClientEmulator c;
	private static transient final Logger LOG = LoggerFactory.getLogger(Controller.class);

	public Controller(ClientEmulator ce) {
		this.c = ce;
	}

	public void run() {
		Date date = new Date();
		LOG.info("Controller is running at " + date.toString());
		String perf = c.getMonitor().readPerformance();
		if (perf != null) {
			/**
			 * Create a rules engine and register the business rule
			 */
			RulesEngine rulesEngine = RulesEngineBuilder.aNewRulesEngine().build();
			ScaleOutRule scaleOutRule = new ScaleOutRule();
			scaleOutRule.setInput(c, perf);
			ScaleInRule scaleInRule = new ScaleInRule();
			scaleInRule.setInput(c, perf);
			rulesEngine.registerRule(scaleOutRule);
			rulesEngine.registerRule(scaleInRule);
			rulesEngine.fireRules();
		}

	}

}
