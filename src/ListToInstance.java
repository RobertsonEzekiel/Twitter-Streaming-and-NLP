import java.util.ArrayList;
import java.util.regex.Pattern;

import cc.mallet.pipe.*;
import cc.mallet.types.*;
import cc.mallet.pipe.iterator.*;

public class ListToInstance {
	Pipe pipe;
	ArrayList<Long> name_id;
	
	public ListToInstance(ArrayList<Long> ids) {
		name_id = ids;
		pipe = buildPipe();
	}
	
	public Pipe buildPipe() {
		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
		pipeList.add(new Input2CharSequence("UTF-8"));
		Pattern tokenPattern = Pattern.compile("[\\p{L}\\p{N}_]+");
		pipeList.add(new CharSequence2TokenSequence(tokenPattern));
		pipeList.add(new TokenSequenceRemoveStopwords(false, false));
		pipeList.add(new TokenSequence2FeatureSequence());
		//pipeList.add(new FeatureSequence2FeatureVector());
		//pipeList.add(new PrintInputAndTarget());
		
		return new SerialPipes(pipeList);
	}
	
	public InstanceList readArray(String[] cleanTexts) {
		StringArrayIterator iterator = new StringArrayIterator(cleanTexts);
		// Construct a new instance list, passing it the pipe we want to use to
		// process instances.
		InstanceList instances = new InstanceList(pipe);
		int index = 0;
		for (Instance inst : instances) {
			inst.setName(name_id.get(index));
			inst.setTarget("english");
			index++;
		}
		// Now process each instance provided by the iterator.
		instances.addThruPipe(iterator);
		return instances;
	}
}
