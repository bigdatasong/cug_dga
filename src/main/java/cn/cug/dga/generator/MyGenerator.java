package cn.cug.dga.generator;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.generator.FastAutoGenerator;
import com.baomidou.mybatisplus.generator.config.rules.DateType;
import com.baomidou.mybatisplus.generator.engine.FreemarkerTemplateEngine;
import org.apache.ibatis.annotations.Mapper;

/**
 * Created by Smexy on 2023/8/21
 *
 *      生成器的原理是逆向工程。
 *
 *      顺向工程:  View---->controller---->service---->Mapper|Dao------->Bean------>表
 *
 *      逆向工程:  表----->反推controller---->service---->Mapper|Dao------->Bean
 */
public class MyGenerator
{
    public static void main(String[] args) {


        //指定为哪些表生成
        String[] tables={"table_meta_info"};

        FastAutoGenerator.create("jdbc:mysql://hadoop102:3306/cug_dga","root","000000")
                         .globalConfig(builder -> {
                             builder.author("song")               //作者
                                    .outputDir("D:\\workspace\\cug_dga\\src\\main\\java")    //输出路径(写到java目录)
                                    .commentDate("yyyy-MM-dd")
                                    .dateType(DateType.SQL_PACK);  //选择实体类中的日期类型  ，Date or LocalDatetime
                         })
                         .packageConfig(builder -> {                 //各个package 名称
                             builder.parent("cn.cug.dga")
                                    .moduleName("meta")
                                    //下面全是父包下的包名
                                    .entity("bean")
                                    .service("service")
                                    .serviceImpl("service.impl")
                                    .controller("controller")
                                    .mapper("mapper");

                         })
                         .strategyConfig(builder -> {
                             builder.addInclude(tables)
                                    .serviceBuilder()
                                    .enableFileOverride()  //生成代码覆盖已有文件 谨慎开启
                                    .formatServiceFileName("%sService")  //类后缀
                                    .formatServiceImplFileName("%sServiceImpl")  //类后缀
                                    .entityBuilder()
                                    .enableFileOverride()
                                    .enableLombok()  //允许使用lombok
                                    .controllerBuilder()
                                    .enableFileOverride()
                                    .formatFileName("%sController")  //类后缀
                                    .enableRestStyle()   //生成@RestController 否则是@Controller
                                    .mapperBuilder()
                                    .enableFileOverride()
                                    //生成通用的resultMap 的xml映射
                                    .enableBaseResultMap()  //生成xml映射
                                    .superClass(BaseMapper.class)  //标配
                                    .formatMapperFileName("%sMapper")  //类后缀
                                    .mapperAnnotation(Mapper.class) ; //生成代码Mapper上自带@Mapper

                         })
                         //自定义模版
                         .templateConfig(
                             builder -> {
                                 // 实体类使用我们自定义模板
                                 builder.entity("templates/myentity.java");
                             })
                         .templateEngine(new FreemarkerTemplateEngine()) // 使用Freemarker引擎模板，默认的是Velocity引擎模板
                         .execute();


    }
}
