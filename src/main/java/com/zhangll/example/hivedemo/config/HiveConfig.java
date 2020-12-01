package com.zhangll.example.hivedemo.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zhangll.example.hivedemo.util.HiveUtil;
import org.apache.hive.jdbc.HiveDriver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.hadoop.hive.HiveClientFactory;
import org.springframework.data.hadoop.hive.HiveClientFactoryBean;
import org.springframework.data.hadoop.hive.HiveTemplate;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;

//import com.mchange.v2.c3p0.ComboPooledDataSource;

@Configuration
public class HiveConfig {
//    @Bean
//    public DataSource dataSource() {
//        SimpleDriverDataSource simpleDriverDataSource = new SimpleDriverDataSource(
//                BeanUtils.instantiateClass(HiveDriver.class),
//                HiveUtil.url,"hadoop","hadoop");
////        simpleDriverDataSource.setDriverClass(HiveDriver.class);
//        return simpleDriverDataSource;
//    }
//

    @Bean
    public DataSource dataSource() throws PropertyVetoException {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass(HiveDriver.class.getName());
        dataSource.setUser("hadoop");
        dataSource.setJdbcUrl(HiveUtil.url);
        return dataSource;
    }


//    @Bean(name = "hiveJdbcDataSource")
//    @Primary
//    @ConfigurationProperties(prefix = "spring.hive")
//    public DataSource hiveDataSource(){
////        DataSource dataSource = DataSourceBuilder.create()
////                .driverClassName(HiveDriver.class.getName())
////                .url(HiveUtil.url)
////                .username("hadoop").build();
////        return dataSource;
//        return DataSourceBuilder.create().build();
//    }

    @Bean("hiveFactory")
    public HiveClientFactory hiveClientFactory(DataSource dataSource) throws Exception {
        HiveClientFactoryBean factoryBean = new HiveClientFactoryBean();
        factoryBean.setHiveDataSource(dataSource);
        // 必须自动添加，否则会出现无
        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }

    @Bean
    @DependsOn("hiveFactory")
    public HiveTemplate hiveTemplate(HiveClientFactory factory){
        HiveTemplate hiveTemplate = new HiveTemplate(factory);
        return hiveTemplate;
    }

}
