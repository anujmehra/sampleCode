package com.poc.java8.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ObjectStreams {

    public static void main(final String args[]){
        
        final ObjectStreams obj = new ObjectStreams();
        
        final List<POJO> list = new ArrayList<>();
        final POJO p1  = new POJO();
        p1.setfName("Anuj");
        p1.setlName("Mehra");
        
        final POJO p2  = new POJO();
        p2.setfName("Anuj2");
        p2.setlName("Mehra2");
        list.add(p1);
        list.add(p2);
        
        obj.findLastName(list);
        
    }
    
    public void findLastName(final List<POJO> pojos){
        
        final List<POJO> myList = pojos.stream().filter(predicate -> predicate.getfName().equals("Anuj"))
                                            .sorted((o1, o2)-> {
                                                return o1.getfName().compareTo(o2.getfName());
                                            })
                                            .collect(Collectors.toList());
                                            //.collect(toCollection(LinkedList::new));

        
        //final Map<String, >
        
    }
    
    private static class POJO {
        private String fName;
        private String lName;
        
        public String getfName() {
        
            return fName;
        }
        public void setfName(final String fName) {
        
            this.fName = fName;
        }
        public String getlName() {
        
            return lName;
        }
        public void setlName(final String lName) {
        
            this.lName = lName;
        }
    }
}
