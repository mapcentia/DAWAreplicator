package com.mapcentia.aws_sync;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: <file.yml> [iffnit]");
            return;
        }

        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(Paths.get(args[0]))) {
            Configuration config = yaml.loadAs(in, Configuration.class);
            System.out.println(config.toString());
        }

        if (args.length > 1 && args[1].equals("init")) {
            ReplicaInit replicaInit = new ReplicaInit();
            replicaInit.start();
        } else if (args.length > 1 && !args[1].equals("init")) {
            System.out.println("Usage: <file.yml> [init]");
        } else {
            ReplicaEvent replicaEvent = new ReplicaEvent();
            replicaEvent.start();
        }
    }
}
