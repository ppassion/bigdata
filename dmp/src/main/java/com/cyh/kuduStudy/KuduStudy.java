package com.cyh.kuduStudy;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class KuduStudy {

    private static KuduClient kuduClient;
    private static String tableName="person";

    @Before
    public void init() {
        //指定master地址
        String masterAddress = "192.168.52.200,192.168.52.210,192.168.52.220";
        //创建kudu的数据库连接
        kuduClient = new KuduClient.KuduClientBuilder(masterAddress).defaultSocketReadTimeoutMs(6000).build();
    }

    public ColumnSchema newColumn(String name, Type type, boolean isKey){
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(isKey);
        return column.build();
    }

    @Test
    public void createTable() throws KuduException {
        //设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn("CompanyId", Type.INT32, true));
        columns.add(newColumn("WorkId", Type.INT32, false));
        columns.add(newColumn("Name", Type.STRING, false));
        columns.add(newColumn("Gender", Type.STRING, false));
        columns.add(newColumn("Photo", Type.STRING, false));

        Schema schema = new Schema(columns);

        //创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        //设置表的副本和分区规则
        LinkedList<String> list = new LinkedList<String>();
        list.add("CompanyId");
        //设置表副本数
        tableOptions.setNumReplicas(1);
        //设置range分区
        //tableOptions.setRangePartitionColumns(list);
        //设置hash分区和分区的数量
        tableOptions.addHashPartitions(list,3);

        try {
            kuduClient.createTable("person",schema,tableOptions);
        } catch (Exception e) {
            e.printStackTrace();
        }

        kuduClient.close();

    }

}
