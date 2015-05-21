package hbase.rule.engine;

import java.util.Collection;

import org.apache.hadoop.hbase.client.Put;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.definition.KnowledgePackage;
import org.drools.io.Resource;
import org.drools.io.ResourceFactory;
import org.drools.runtime.StatelessKnowledgeSession;


public class RuleEngine {

	//private RuleBase rules;
	private KnowledgeBase _kbase;
	private StatelessKnowledgeSession _ksession;
	
	public RuleEngine(String rulesFile) throws RuleEngineException {

		try {

			KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
			
			// this will parse and compile the DRL file
			Resource r = ResourceFactory.newFileResource(rulesFile);
			kbuilder.add(r, ResourceType.DRL);

			// Check the builder for errors
			if (kbuilder.hasErrors()) {
				System.out.println(kbuilder.getErrors().toString());
				throw new RuntimeException("Unable to compile the DRL file: "
						+ rulesFile);
			}

			// get the compiled packages
			final Collection<KnowledgePackage> pkgs = kbuilder.getKnowledgePackages();

			// add the packages to a KnowledgeBase (deploy the knowledge
			// packages).
			_kbase = KnowledgeBaseFactory.newKnowledgeBase();
			_kbase.addKnowledgePackages(pkgs);
			_ksession = _kbase.newStatelessKnowledgeSession();

		} catch (Exception e) {
			throw new RuleEngineException("Could not load/compile rules from DRL file: '" 
		                                 + rulesFile+ "' ", e);
		}
	}
	
	public RuleEngine(byte[] rule) throws RuleEngineException {

		try {

			KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
			
			// this will parse and compile the DRL file
			Resource r = ResourceFactory.newByteArrayResource(rule);
			kbuilder.add(r, ResourceType.DRL);

			// Check the builder for errors
			if (kbuilder.hasErrors()) {
				System.out.println(kbuilder.getErrors().toString());
				throw new RuntimeException("Unable to compile the Rule");
			}

			// get the compiled packages
			final Collection<KnowledgePackage> pkgs = kbuilder.getKnowledgePackages();

			// add the packages to a KnowledgeBase (deploy the knowledge
			// packages).
			_kbase = KnowledgeBaseFactory.newKnowledgeBase();
			_kbase.addKnowledgePackages(pkgs);
			_ksession = _kbase.newStatelessKnowledgeSession();

		} catch (Exception e) {
			throw new RuleEngineException("Could not load/compile rules " , e);
		}
	}

	public void executeRules(Put put) {
		
		_ksession.execute(put);
	}
}