/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.lucene.Lucene;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public class UniqueTokenFilterTests {

    @Test
    public void simpleTest() throws IOException {
        Analyzer analyzer = new ReusableAnalyzerBase() {
            @Override
            protected Analyzer.TokenStreamComponents createComponents(String fieldName,
                                                             Reader reader) {
                Tokenizer t = new WhitespaceTokenizer(Lucene.VERSION, reader);
                return new TokenStreamComponents(t, new UniqueTokenFilter(t));
            }
        };

        TokenStream test = analyzer.tokenStream("test", new StringReader("this test with test"));
        CharTermAttribute termAttribute = test.addAttribute(CharTermAttribute.class);
        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("this"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("test"));

        assertThat(test.incrementToken(), equalTo(true));
        assertThat(termAttribute.toString(), equalTo("with"));

        assertThat(test.incrementToken(), equalTo(false));
    }
}
