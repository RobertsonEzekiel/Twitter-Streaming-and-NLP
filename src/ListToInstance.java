import java.util.ArrayList;
import java.util.regex.Pattern;

import cc.mallet.pipe.*;
import cc.mallet.types.*;
import cc.mallet.pipe.iterator.*;

public class ListToInstance {
	Pipe pipe;
	ArrayList<Long> name_id;
	Alphabet alphabet;
	
	public ListToInstance(ArrayList<Long> ids, Alphabet ABC) {
		name_id = ids;
		pipe = buildPipe();
		alphabet = ABC;
		//Debug
		if (alphabet != null) {
			System.out.println("Alphabet size: " + alphabet.size());
		}
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
		SerialPipes serialPipeList = new SerialPipes(pipeList);
		serialPipeList.setDataAlphabet(alphabet);
		return serialPipeList;
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
