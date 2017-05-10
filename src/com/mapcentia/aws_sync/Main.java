package com.mapcentia.aws_sync;

public class Main {
    public static void main(String args[]) throws Exception {
        ReplicaEvent replicaEvent = new ReplicaEvent();
        replicaEvent.start();

        //ReplicaInit replicaInit = new ReplicaInit();
        //replicaInit.start();
    }
}
