package com.github.akoshchiy;

import com.github.akoshchiy.datafusion.Runtime;
import com.github.akoshchiy.datafusion.SessionContext;

/**
 * Hello world!
 *
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Runtime rt = Runtime.create();
//        SessionContext ctx = SessionContext.create(rt);
//        ctx.close();

        System.out.println("Done!");
    }
}
