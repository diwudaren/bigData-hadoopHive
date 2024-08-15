package com;

import org.junit.Test;

public class TestDeamon {

    @Test
    public void replaceTest(){
        String s = "aaa,bbb,ccc,ddd".replaceAll("[^,]+", "$0 varchar");
        System.out.println(s);
    }
}
