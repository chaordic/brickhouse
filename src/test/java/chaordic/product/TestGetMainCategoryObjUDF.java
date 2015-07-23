package chaordic.product;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by mateus on 7/15/15.
 */
public class TestGetMainCategoryObjUDF {

    private GetMainCategoryUDF udf;
    private static List<String> CATEGORY_FIELD_NAMES = Lists.newArrayList("parents", "id", "name");
    private static List<ObjectInspector> CATEGORY_FIELD_TYPES = Lists.newArrayList(
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
    );
    private ObjectInspector[] argumentInspector;

    @Before
    public void setUp() {
        this.udf = new GetMainCategoryUDF();
        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(CATEGORY_FIELD_NAMES, CATEGORY_FIELD_TYPES);
        ListObjectInspector loi = ObjectInspectorFactory.getStandardListObjectInspector(soi);
        this.argumentInspector = new ObjectInspector[] { loi };
    }

    @Test
    public void shouldReturnNull_whenCategoriesIsNull() throws HiveException {
        this.udf.initialize(this.argumentInspector);

        List categories = null;
        DeferredJavaObject deferredCategories = new DeferredJavaObject(categories);

        String rootCategory = this.udf.evaluate(new DeferredObject[]{deferredCategories});
        assertNull(rootCategory);
    }

    @Test
    public void shouldReturnTheRootCategory_whenThereIsOnlyOne() throws HiveException {
        this.udf.initialize(this.argumentInspector);

        List categories = createCategories();
        categories.add(Lists.newArrayList(null, "a", "1"));
        DeferredJavaObject deferredCategories = new DeferredJavaObject(categories);

        String rootCategory = this.udf.evaluate(new DeferredObject[]{deferredCategories});
        assertEquals("[null, a, 1]", rootCategory);
    }

    @Test
    public void shouldReturnTheRootCategory_whenParentsIsEmptyList() throws HiveException {
        this.udf.initialize(this.argumentInspector);

        List categories = createCategories();
        categories.add(Lists.newArrayList(new ArrayList(), "a", "1"));
        DeferredJavaObject deferredCategories = new DeferredJavaObject(categories);

        String rootCategory = this.udf.evaluate(new DeferredObject[]{ deferredCategories });
        assertEquals("[[], a, 1]", rootCategory);
    }

    @Test
    public void shouldReturnTheRootCategory_whenParentsIsEmptyString() throws HiveException {
        this.udf.initialize(this.argumentInspector);

        List categories = createCategories();
        categories.add(Lists.newArrayList("", "a" , "1"));
        DeferredJavaObject deferredCategories = new DeferredJavaObject(categories);

        String rootCategory = this.udf.evaluate(new DeferredObject[]{ deferredCategories });
        assertEquals("[, a, 1]", rootCategory);
    }

    @Test
    public void shouldReturnTheRootCategory_whenParentsIsTheEmptyListString() throws HiveException {
        this.udf.initialize(this.argumentInspector);

        List categories = createCategories();
        categories.add(Lists.newArrayList("[]", "a" , "1"));
        DeferredJavaObject deferredCategories = new DeferredJavaObject(categories);

        String rootCategory = this.udf.evaluate(new DeferredObject[]{ deferredCategories });
        assertEquals("[[], a, 1]", rootCategory);
    }

    private List createCategories() {
        List category = Lists.newArrayList("aParent", "aCategory", "aCategoryId");
        List categories = new ArrayList();
        categories.add(category);
        categories.add(null);
        return categories;
    }

}
