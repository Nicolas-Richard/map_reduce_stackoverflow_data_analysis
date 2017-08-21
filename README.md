Stack overflow provides its data here : https://archive.org/download/stackexchange

The "comments" data unzipped is about 15GB.

## What is the most referenced Wikipedia page in Stackoverflow comments ?

# 1. Naively parse the URLs

For each comment, I check if there is a Wikipedia link inside. I parse it out and sent it to the reducer.

The difficult part is the links are not nicely tagged but included in plain format in the body of the comment. 

The problem with this approach is I'm getting a lot of garbage URLs because the links are not nicely tagged but included in plain format in the body of the comment which make parsing them not obvious.

This is a major problem as urls have a tendency to be found at the end of a sentence or between parentheses, or followed by a comma, a dot, any punctuation mark or a permutation of all / any of that.

The same page can have several representations...

https://en.wikipedia.org/wiki/SQL_injection
https://en.wikipedia.org/wiki/SQL_injection)
https://en.wikipedia.org/wiki/SQL_injection).

Note: In some cases a parenthesi at the end of a URL is legit and should not be removed...

# 2. Check that the URLs are reachable

Before emitting a URL to the reducer I need to make sure it's a good one.

Let's make a GET request to every candidate page, if I receive a 200 status code as a response, I'll consider this URL good.

This is slow... And of course turns out to be very inefficient as it means I connect to the same popular Wikipedia pages over and over again just to figure out that "yes it's a good one".

I was able to run the job this way for a subset of the dataset (first 10k records). The result is a list of validated URLs that we will be re-using later.

# 3. rule of thumb to recover dirty URLs

 As I run the job this way I realized I was getting a lot of 404 responses due to the poorly shaped URLs with punctuation marks at the end. More than half of the links were discarded for that reason.

I decided that for every candidate URL I will be testing :

- The URL as parsed
- The URL minus the last character
- The URL minus the 2 last characters

Now I was catching most URLs but processing got very very slow.

# 4. Distributed Cache

To avoid this the Distributed Cache feature of Map Reduce is on point.

Each mapper upon start will load in-memory a set of knowns good URLs that we obtained the hard way, by connecting to each one and getting the desired 200 response.

With this is place, to validate a URL, first check if it's in the cache.

If it's not, try to connect to it.

My first cache file was created from processing the 10k records, then I created one from 1 million records, then one from 20 million records (about 30% of the data), and from that I was able to analyze the entire dataset in a few hours on my laptop.


# 5. Gather some stats

Using the Map Reduce context counters it's very easy to count what is going on at every step.

Status 200  11109
Status 400  1030
Status 404  11341
All records scans 58159098
Cache_Hits  64364
Wikipedia_URL_Found 77284
unique URLS 16103

From this we can see that the cache has proven very useful
cache hit ratio : 83%

200 + cache hits : 75473

Urls found that could not be understood / resolved : 1811 / 2%

# 6. The results

This is the top 50 of most common Wikipedia pages found in StackOverflow comments.

Urls / Count

1. https://en.wikipedia.org/wiki/Prepared_statement 1760
2. https://en.wikipedia.org/wiki/SQL_injection 1213
3. https://en.wikipedia.org/wiki/Same_origin_policy 577
4. https://en.wikipedia.org/wiki/Database_normalization 444
5. https://en.wikipedia.org/wiki/Undefined_behavior 399
6. https://en.wikipedia.org/wiki/Byte_order_mark 304
7. https://en.wikipedia.org/wiki/Singleton_pattern 260
8. https://en.wikipedia.org/wiki/ISO_8601 259
9. https://en.wikipedia.org/wiki/Floating_point 249
10. https://en.wikipedia.org/wiki/Same-origin_policy 231
11. https://en.wikipedia.org/wiki/JSONP 230
12. https://en.wikipedia.org/wiki/Model%E2%80%93view%E2%80%93controller 215
13. https://en.wikipedia.org/wiki/Endianness 208
14. https://en.wikipedia.org/wiki/Big_O_notation 208
15. https://en.wikipedia.org/wiki/Hash_table 204
16. https://en.wikipedia.org/wiki/Liskov_substitution_principle 199
17. https://en.wikipedia.org/wiki/Post/Redirect/Get 198
18. https://en.wikipedia.org/wiki/Regular_expression 189
19. https://en.wikipedia.org/wiki/Base64 186
20. https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle 183
21. https://en.wikipedia.org/wiki/Cross-site_scripting 178
22. https://en.wikipedia.org/wiki/UTF-8 174
23. https://en.wikipedia.org/wiki/Levenshtein_distance 173
24. https://en.wikipedia.org/wiki/Knapsack_problem 173
25. https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern 173
26. https://en.wikipedia.org/wiki/Cross-origin_resource_sharing 173
27. https://en.wikipedia.org/wiki/Trie 172
28. https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes 171
29. https://en.wikipedia.org/wiki/Single_responsibility_principle 169
30. https://en.wikipedia.org/wiki/Bitwise_operation 169
31. https://en.wikipedia.org/wiki/Unobtrusive_JavaScript 159
32. https://en.wikipedia.org/wiki/Factory_method_pattern 158
33. https://en.wikipedia.org/wiki/List_of_HTTP_status_codes 154
34. https://en.wikipedia.org/wiki/Data_URI_scheme 154
35. https://en.wikipedia.org/wiki/Short-circuit_evaluation 153
36. https://en.wikipedia.org/wiki/Newline 153
37. https://en.wikipedia.org/wiki/Don't_repeat_yourself 150
38. https://en.wikipedia.org/wiki/Sorting_algorithm 144
39. https://en.wikipedia.org/wiki/JSON 142
40. https://en.wikipedia.org/wiki/Comma-separated_values 139
41. https://en.wikipedia.org/wiki/C%2B%2B11 135
42. https://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization 133
43. https://en.wikipedia.org/wiki/Modulo_operation 133
44. https://en.wikipedia.org/wiki/Most_vexing_parse 131
45. https://en.wikipedia.org/wiki/Dependency_injection 127
46. https://en.wikipedia.org/wiki/ASCII 124
47. https://en.wikipedia.org/wiki/First_normal_form 123
48. https://en.wikipedia.org/wiki/Return_value_optimization 121
49. https://en.wikipedia.org/wiki/Observer_pattern 121
50. https://en.wikipedia.org/wiki/Unix_time 120

Inspired by "MapReduce Design Patterns" by Donald Miner, Adam Shook http://shop.oreilly.com/product/0636920025122.do

I'm using the MRDPutils module from the book, can be found here : https://github.com/adamjshook/mapreducepatterns/blob/master/MRDP/src/main/java/mrdp/utils/MRDPUtils.java
