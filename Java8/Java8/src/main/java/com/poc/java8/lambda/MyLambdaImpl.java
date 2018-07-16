package com.poc.java8.lambda;

public class MyLambdaImpl {

    public void testAnonymous(){
        //Defining an analymous class
        final LamdbaInterface1 obj = new LamdbaInterface1(){

            @Override
            public String method1(final String str1, final String str2) {

                return str1 + str2;
            }
        };

        final String str = obj.method1("str1", "str2");
        System.out.println(str);
    }

    
    public void testLambda(){
        final LamdbaInterface1 obj = ((final String str1, final String str2) -> {
            return str1+ str2;
        });
        
        final String str = obj.method1("str1", "str2");
        System.out.println(str);
    }

    
    public static void main(final String args[]){
        final MyLambdaImpl obj = new MyLambdaImpl();
        obj.testAnonymous();
        obj.testLambda();
    }
}
