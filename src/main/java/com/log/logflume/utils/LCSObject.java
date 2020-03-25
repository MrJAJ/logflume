package com.log.logflume.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LCSObject {
	protected List<Integer> lineIds = new ArrayList<Integer>(); //Holds line ids
	protected String LCSseq[]; //Token Sequence
    protected Map<Integer,List<String>> params=new HashMap<>();
	
	public LCSObject(String[] seq, int lineId) {
		LCSseq = seq;
		lineIds.add(lineId);
		params.put(lineId,new ArrayList<>());

	}
	
	//Get the length of the LCS between sequences
	public int getLCS(String[] seq) {
		int count = 0;
		
		//Loop through current sequence using the simple loop approach described in the paper
		int lastMatch = -1;
		for(int i = 0; i < LCSseq.length; i++) {
			if(LCSseq[i].equals("*")) {
				continue;
			}
			
			for(int j = lastMatch + 1; j < seq.length; j++) {
				if(LCSseq[i].equals(seq[j])) {
					lastMatch = j;
					count++;
					break;
				}
			}
		}
		
		return count;
	}
	
	//Insert a line into the LCSObject
	public void insert(String[] seq, int lineId) {		
		lineIds.add(lineId);
        params.put(lineId,new ArrayList<>());
		String temp = "";
		
		//Create the new sequence by looping through it
		int lastMatch = -1;
		boolean placeholder = false; //Decides whether or not to add a * depending if there is already one preceding or not
		for(int i = 0; i < LCSseq.length; i++) {

			if(LCSseq[i].equals("*")) {
				if(!placeholder) {
					temp = temp + "* ";
				}
				placeholder = true;
				continue;
			}
			for(int j = lastMatch + 1; j < seq.length; j++) {
				if(LCSseq[i].equals(seq[j])) {
					StringBuilder b=new StringBuilder();
					for(int k=lastMatch+1;k<j;k++) {
						b.append(seq[k]);
					}
					if(b.length()>0) {
						params.get(lineId).add(b.toString());
					}
					placeholder = false;
					temp = temp + LCSseq[i] + " ";
					lastMatch = j;
					break;
				}else if(!placeholder) {
					temp = temp + "* ";
					placeholder = true;
				}
			}
		}
		if(lastMatch+1<seq.length){
			if(!placeholder) {
				temp = temp + "* ";
			}
			StringBuilder b=new StringBuilder();
			for(int k=lastMatch+1;k<seq.length;k++) {
				b.append(seq[k]);
			}
			if(b.length()>0) {
				params.get(lineId).add(b.toString());
			}
		}
		//Set sequence based of the common sequence found

        String oldSeq=LCSseq.toString();
        LCSseq = temp.trim().split("[\\s]+");
	}
	
	//Length for pruning
	public int length() {
		return LCSseq.length;
	}
	
	//Count of lineIds in this LCSObject
	public int count() {
		return lineIds.size();
	}
	
	//To String method for testing
	@Override
	public String toString() {
		String temp = "";
		
		for(String s : LCSseq) {
			temp = temp + s + " ";
		}
		
		temp = temp + "\t"+lineIds.size();//+"\n\t参数列表:";//"\n\t\t{";
//		for(int i : lineIds) {
//		    if(params.get(i).size()!=0){
//                temp = temp + params.get(i) + "\t";
//            }
//
//		}
		
	//	return temp.substring(0, temp.length() - 2) + "}";
        return temp;
	}
}
