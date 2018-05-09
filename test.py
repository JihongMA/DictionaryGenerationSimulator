from Leader import *
from Bully import *
from File_producer import *
from Local import *
from MostlyOP import *

import argparse
from os import path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate a 1.x-pass dictionary encoding.\n "
                                                 'File producer loads given file as word bank and produces intermediate files according to the given configuration(eg. word count, word distribution, file layout, etc). '
                                                 'These intermediate files are set as input for dictionary encoding simulation.')

    parser.add_argument('--file', dest='file', default='/usr/share/dict/words', help='The word bank file from which dictionary keys is loaded, defaults to /usr/share/dict/words')
    parser.add_argument('--filelayout', dest='filelayout', choices=['column','table'], default='column', help='File producer uses the given file as word bank to make one column file or multiple column table file, defaults to column')
    parser.add_argument('--workers', action='append', dest='workers', type=int, default=[10], help='Number of workers for each experiment combination, defaults to array={10,}')
    parser.add_argument('--column', dest='column', type=int, default=0, help='Column number for target field to be encoded in generated table file, defaults to 0')
    parser.add_argument('--protocol', dest='protocol', choices=['leader','bully','local','mostlyop'], default='mostlyop', help='The consensus protocol, defaults to leader, defaults to leader')
    # If we need more distinct words than that in bank file, we can use randomly generated string as new words. You can get column with this feature in 'table' layout file.
    parser.add_argument('--wordcount', action='append', type=int, default=[400000], help='Total number of words in all intermediate files for each experiment combination, defaults to array={40000,}')
    parser.add_argument('--distribution', dest='distribution', default='zipf', choices=['weighted','uniform','zipf'], help='The distribution (uniform/zipf) of words in input files, defaults to zipf')
    parser.add_argument('--zipfarg', action='append', type=float, default=[2.0], help='Parameter for zipf distribution when zipf is used, defaults to 2.0')
    parser.add_argument("--batch", action='append', type=int, default=[16], help='Number of new keys in each new key proposal for each experiment combination,defaults to array={16,}')
    # Latency with norm distribution is added to each message
    parser.add_argument('--delay', action ='append', dest='delay', type=float, default=[0] , help='Latency(s) with norm distribution is added to each message for each experiment combination,  defaults to array={0}')
    #parser.add_argument("--lookahead", action='append', type=float, default=[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0], help='Portion of the file to look ahead for each experiment combination, defaults to array={0.0, 0.1, ..., 1.0}')
    parser.add_argument("--lookahead", action='append', type=float, default=[0.3], help='Portion of the file to look ahead for each experiment combination, defaults to array={0.0, 0.1, ..., 1.0}')
    parser.add_argument("--order", action ='store_false', default=True, help='Prebuild dictionary with or without order')
    parser.add_argument("--ratio", action='append', type=float, default=[1.0], help='Portion of word bank used to build the file, defaults to array={0.0, 0.1, ..., 1.0}')
    parser.add_argument("--scale", action='append', type=float, default=[1], help='Scalar for original interval used between two consecutive value')

    args = parser.parse_args()

    if not path.exists(args.file):
        print("Error, path %s does not exist" % args.file)
        exit()
    # Validation checks finished

    #output_file = path.join("results","results_%s_%s.csv" %(args.protocol, args.distribution))
    output_file = path.join("results","results.csv")
    print("Using Dictionary Input File: %s Results Output File in csv format:%s\n" %(args.file, output_file))
    print("params: %s \n" % args)
    with open(args.file, "r") as f:
        words = f.read().split("\n")

    ratios_list = args.ratio # r is used to adjust the duplicate words in generated files
    num_change_words_trials = 1 # number of trials with different generated files (No need to add it into parser as a configuable argument)
    num_inner_trials = 10 # in order to get average processing performance evaluation the dictionary simulator is given times (No need to add it into parser as a configuable argument)
    num_total_trials = num_change_words_trials * num_inner_trials # total number of rounds for each config.

    with open(output_file, "w+") as outfile:
        header = "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}\n".format(
                                    "num_words", "worker_num", "buffer_size", "delay", "look_ahead",
                                    "avg_num_messages_sent", "avg_time_elapsed(s)", "time_for_lookahead(s)", "time_for_dict_encoding(s)", "num_key_conflict", "num_abort")
        outfile.write(header)

        # Loop over all parameters...
        # Loop through each word count combination
        for zipf in args.zipfarg:
            for num_words in args.wordcount:
                for worker_num in args.workers:
                    num_words_per_file = int(num_words / worker_num)
                    for scale in args.scale:
                        for ratio in ratios_list:
                            # File is generated based on parameters computed above
                            # Preprocessing: generating the input files with configuable distribution for dictionary simulation
                            if args.filelayout == 'column':
                                files = make_file(words[:int(len(words)*ratio)], worker_num, num_words_per_file, zipf, args.distribution)
                            else:
                                files = make_sample_table(words[:int(len(words)*ratio)], worker_num, num_words_per_file)
                            for look in args.lookahead:
                                for delay in args.delay:
                                    for buffer_size in args.batch:
                                        # Compute indicators like time elapsed, time for lookahead, time for dictionary encoding, number of message sent, number of key conflict, number of aborts
                                        elapsed_overall_agg = 0
                                        tlook_overall_agg = 0
                                        tdict_overall_agg = 0
                                        num_messages_sent_overall_agg = 0
                                        num_key_conflict_overall_agg = 0
                                        num_abort_overall_agg = 0
                                        for change_words_trial in range(1, 1 + num_change_words_trials):
                                            elapsed_cur_agg = 0
                                            tlook_cur_agg = 0
                                            tdict_cur_agg = 0
                                            num_messages_sent_cur_agg = 0
                                            num_key_conflict_cur_agg = 0
                                            num_abort_cur_agg = 0

                                            for inner_trial in range(1, 1 + num_inner_trials):
                                                # Run coresponding protocol tester according args.protocol
                                                if args.protocol == "leader":
                                                    tester = Leader_tester(worker_num, files, buffer_size, delay, look, num_words_per_file, args.column, args.order, scale)

                                                elif args.protocol == "local":
                                                    tester = Local_tester(worker_num, files, buffer_size, delay, look, num_words_per_file, args.column, args.order, scale)

                                                elif args.protocol == "bully":
                                                    tester = Bully_2PC_tester(worker_num, files, buffer_size, delay, look, num_words_per_file, args.column, args.order, scale)

                                                elif args.protocol == "mostlyop":
                                                    tester = MostlyOP_tester(worker_num, files, buffer_size, delay, look, num_words_per_file, args.column, args.order, scale)

                                                else:
                                                    raise ValueError("Invalid input protocol \"{}\" is still not supported".format(args.protocol))

                                                elapsed, tlook, tdict, num_messages_sent, num_key_conflict, num_abort = tester.run_test();

                                                elapsed_cur_agg += elapsed
                                                elapsed_overall_agg += elapsed

                                                tlook_cur_agg += tlook
                                                tlook_overall_agg += tlook

                                                tdict_cur_agg += tdict
                                                tdict_overall_agg += tdict

                                                num_messages_sent_cur_agg += num_messages_sent
                                                num_messages_sent_overall_agg += num_messages_sent

                                                num_key_conflict_cur_agg += num_key_conflict
                                                num_key_conflict_overall_agg += num_key_conflict

                                                num_abort_cur_agg += num_abort
                                                num_abort_overall_agg += num_abort


                                        to_print = "w/n = {}/{}, buffer size = {}, delay = {}, lookahead = {},average over all Trials: {} messages sent, {} time elapsed, {} lookahead, {} encoding, {} key conflicts, {} aborts, {} word bank, {} zipfarg\n".format(
                                            num_words, worker_num, buffer_size, delay, look,
                                            num_messages_sent_overall_agg / num_total_trials, elapsed_overall_agg / num_total_trials, tlook_overall_agg / num_total_trials, tdict_overall_agg / num_total_trials, num_key_conflict_overall_agg / num_total_trials, num_abort_overall_agg / num_total_trials,int(len(words)*ratio),zipf)
                                        to_write = "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}\n".format(
                                            num_words, worker_num, buffer_size, delay, look,
                                            num_messages_sent_overall_agg / num_total_trials, elapsed_overall_agg / num_total_trials, tlook_overall_agg / num_total_trials, tdict_overall_agg / num_total_trials, num_key_conflict_overall_agg / num_total_trials, num_abort_overall_agg / num_total_trials,int(len(words)*ratio),zipf)
                                        print(to_print)
                                        outfile.write(to_write)
                                        # Output all indicator for performance evaluation

    outfile.close()
