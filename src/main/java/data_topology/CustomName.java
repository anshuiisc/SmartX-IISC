package data_topology;

import backtype.storm.task.TopologyContext;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;

import java.util.Map;

/**
 * Replace this line with a description of what this class does.
 * Feel free to be verbose and descriptive for key classes.
 *
 * @author Arun Verma [mailto:arunverma100@gmail.com]
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 * <p/>
 * Copyright 2015 DREAM:Lab, Indian Institute of Science, Bangalore
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class CustomName implements FileNameFormat {
//    private String componentId;
//    private int taskId;
    private int fileId = 0;
    private String path = "/waterData";
    private String prefix = "";
    private String extension = ".txt";

    public CustomName() {
    }

    public CustomName withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public CustomName withExtension(String extension) {
        this.extension = extension;
        return this;
    }

    public CustomName withPath(String path) {
        this.path = path;
        return this;
    }

    public void prepare(Map conf, TopologyContext topologyContext) {
//        this.componentId = topologyContext.getThisComponentId();
//        this.taskId = topologyContext.getThisTaskId();
    }

    public String getName(long rotation, long timeStamp) {
//        return this.prefix + this.componentId + "-" + this.taskId + "-" + rotation + "-" + timeStamp + this.extension;
        this.fileId = this.fileId + 1 ;
        return this.prefix + "-" + this.fileId + "-" + timeStamp + this.extension;

    }

    public String getPath() {
        return this.path;
    }
}