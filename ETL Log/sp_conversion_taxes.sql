------------------
select 'to_date(''' ||
       to_char (
          nvl (max (closedtaxperiod_1.end_date),
               to_date ('19000101', 'yyyyMMdd')),
          'yyyyMMdd')||
       ''', ''yyyyMMdd'')'
          as closedperiod
from   dm_taxs.closedtaxperiod closedtaxperiod_1
where  (closedtaxperiod_1.deleted_flag = 'N') and
       (closedtaxperiod_1.closed_flag = 'Y')
------------------
select 'PARTITION' as ml_partition,
       'BY' as ml_by,
       'OVER' as ml_over,
       'ORDER' as ml_order,
       'ASC' as ml_asc,
       'DESC' as ml_desc,
       'NULLS LAST' as ml_nulls_last,
       'NULLS FIRST' as ml_nulls_first
from   dual
------------------
select currency_2.uk as currency_uk
from   (select currency_1.iso_ccode as iso_ccode,
               max (currency_1.as_of_day) as as_of_day
        from   main.currency currency_1
        where  (nvl (currency_1.default_flag, 'N') != 'Y') and
               (nvl (currency_1.deleted_flag, 'N') != 'Y')
        group by currency_1.iso_ccode) iso_ccode
       inner join main.currency currency_2
          on iso_ccode.iso_ccode = currency_2.iso_ccode and
             iso_ccode.as_of_day = currency_2.as_of_day and
             nvl (currency_2.default_flag, 'N') != 'Y' and
             nvl (currency_2.deleted_flag, 'N') != 'Y'
where  (upper (trim (iso_ccode.iso_ccode)) = 'RUR')
------------------
declare
begin
   dm_taxs.truncate_table ('CONVPURCHASE_PROC');
end;
------------------
insert into dm_taxs.convpurchase_proc (deal_back_ref,
                                       deal_couponaccount_cur_amt,
                                       deal_couponacct_cur_old_amt,
                                       deal_cur_amt,
                                       deal_cur_old_amt,
                                       deal_uk,
                                       isin,
                                       issuesecurity_series,
                                       issuesecurity_uk,
                                       link_ncode,
                                       oper_date,
                                       pay_cur_amt,
                                       pay_old_amt,
                                       reg_actual_date,
                                       revaluation_rur_amt,
                                       security_cnt,
                                       comission_rur_amt,
                                       currency_issue_uk,
                                       currency_pay_uk,
                                       currency_uk)
   select convpurchase_proc.deal_back_ref as deal_back_ref,
          convpurchase_proc.deal_couponaccount_cur_amt
             as deal_couponaccount_cur_amt,
          convpurchase_proc.deal_couponacct_cur_old_amt
             as deal_couponacct_cur_old_amt,
          convpurchase_proc.deal_cur_amt as deal_cur_amt,
          convpurchase_proc.deal_cur_old_amt as deal_cur_old_amt,
          convpurchase_proc.deal_uk as deal_uk,
          convpurchase_proc.isin as isin,
          convpurchase_proc.issuesecurity_series as issuesecurity_series,
          convpurchase_proc.issuesecurity_uk as issuesecurity_uk,
          convpurchase_proc.link_ncode as link_ncode,
          convpurchase_proc.oper_date as oper_date,
          convpurchase_proc.pay_cur_amt as pay_cur_amt,
          convpurchase_proc.pay_old_amt as pay_old_amt,
          convpurchase_proc.reg_actual_date as reg_actual_date,
          convpurchase_proc.revaluation_rur_amt as revaluation_rur_amt,
          convpurchase_proc.security_cnt as security_cnt,
          convpurchase_proc.comission_rur_amt as comission_rur_amt,
          convpurchase_proc.currency_issue_uk as currency_issue_uk,
          convpurchase_proc.currency_pay_uk as currency_pay_uk,
          convpurchase_proc.currency_uk as currency_uk
   from   (select convsumrealiz.link_ncode as link_ncode,
                  dealsecurity_act_14.deal_uk as deal_uk,
                  dealsecurity_act_14.issuesecurity_uk
                     as issuesecurity_uk,
                  dealsecurity_act_14.oper_date as oper_date,
                  dealsecurity_act_14.currency_issue_uk
                     as currency_issue_uk,
                  dealsecurity_act_14.currency_pay_uk
                     as currency_pay_uk,
                  dealsecurity_act_14.currency_uk as currency_uk,
                  dealsecurity_act_14.reg_actual_date
                     as reg_actual_date,
                  dealsecurity_act_14.deal_back_ref as deal_back_ref,
                  issuesecurity_act_16.issuesecurity_series
                     as issuesecurity_series,
                  issuesecurity_act_16.isin as isin,
                  dealsecurity_act_14.security_cnt as security_cnt,
                  dealsecurity_act_14.deal_cur_amt as deal_cur_old_amt,
                  dealsecurity_act_14.deal_couponaccount_cur_amt
                     as deal_couponacct_cur_old_amt,
                  dealsecurity_act_14.pay_amt as pay_old_amt,
                  convsumrealiz.deal_rur_amt /
                  sum (dealsecurity_act_14.security_cnt)
                     over (partition by convsumrealiz.link_ncode) *
                  dealsecurity_act_14.security_cnt /
                  exchangexrate_15.rate
                     as deal_cur_amt,
                  convsumrealiz.deal_couponaccount_rur_amt /
                  sum (dealsecurity_act_14.security_cnt)
                     over (partition by convsumrealiz.link_ncode) *
                  dealsecurity_act_14.security_cnt /
                  exchangexrate_15.rate
                     as deal_couponaccount_cur_amt,
                  convsumrealiz.pay_rur_amt /
                  sum (dealsecurity_act_14.security_cnt)
                     over (partition by convsumrealiz.link_ncode) *
                  dealsecurity_act_14.security_cnt /
                  exchangexrate_15.rate
                     as pay_cur_amt,
                  convsumrealiz.revaluation_rur_amt /
                  sum (dealsecurity_act_14.security_cnt)
                     over (partition by convsumrealiz.link_ncode) *
                  dealsecurity_act_14.security_cnt
                     as revaluation_rur_amt,
                  convsumrealiz.comission_rur_amt /
                  sum (dealsecurity_act_14.security_cnt)
                     over (partition by convsumrealiz.link_ncode) *
                  dealsecurity_act_14.security_cnt
                     as comission_rur_amt
           from   (select convdeal.link_ncode as link_ncode,
                          sum (
                             dealsecurity_act_6.deal_cur_amt /
                             dealsecurity_act_6.security_cnt *
                             taxpair_4.security_cnt *
                             exchangexrate_7.rate)
                             as deal_rur_amt,
                          sum (
                             dealsecurity_act_6.deal_couponaccount_cur_amt /
                             dealsecurity_act_6.security_cnt *
                             taxpair_4.security_cnt *
                             exchangexrate_7.rate)
                             as deal_couponaccount_rur_amt,
                          sum (
                             dealsecurity_act_6.pay_amt /
                             dealsecurity_act_6.security_cnt *
                             taxpair_4.security_cnt *
                             exchangexrate_7.rate)
                             as pay_rur_amt,
                          sum (
                             acctrevaluation2005_lstat_9.revaluation_cur_amt /
                             dealsecurity_act_6.security_cnt *
                             taxpair_4.security_cnt *
                             exchangexrate_10.rate)
                             as revaluation_rur_amt,
                          nvl (
                             sum (
                                dealoperacct_act_11.opernotvat_cur_amt *
                                exchangexrate_12.rate) /
                             dealsecurity_act_6.security_deal_cnt *
                             taxpair_4.security_cnt,
                             0)
                             as comission_rur_amt
                   from   (select exp_pre_convdeal.allpair_flag
                                     as allpair_flag,
                                  exp_pre_convdeal.deal_uk as deal_uk,
                                  exp_pre_convdeal.issuesecurity_uk
                                     as issuesecurity_uk,
                                  exp_pre_convdeal.link_ncode as link_ncode,
                                  exp_pre_convdeal.security_cnt
                                     as security_cnt,
                                  exp_pre_convdeal.security_pair_cnt
                                     as security_pair_cnt
                           from   (select dealsecurity_act_2.deal_uk
                                             as deal_uk,
                                          dealsecurity_act_2.issuesecurity_uk
                                             as issuesecurity_uk,
                                          dealconversion_1.link_ncode
                                             as link_ncode,
                                          dealsecurity_act_2.security_cnt
                                             as security_cnt,
                                          nvl (
                                             sum (
                                                taxpair_3.security_cnt),
                                             0)
                                             as security_pair_cnt,
                                          case
                                             when dealsecurity_act_2.security_cnt >
                                                     nvl (
                                                        sum (
                                                           taxpair_3.security_cnt),
                                                        0)
                                             then
                                                'N'
                                             else
                                                'Y'
                                          end
                                             as allpair_flag,
                                          count (
                                             case
                                                when (case
                                                         when dealsecurity_act_2.security_cnt >
                                                                 nvl (
                                                                    sum (
                                                                       taxpair_3.security_cnt),
                                                                    0)
                                                         then
                                                            'N'
                                                         else
                                                            'Y'
                                                      end) = 'N'
                                                then
                                                   1
                                                else
                                                   null
                                             end)
                                          over (
                                             partition by dealconversion_1.link_ncode)
                                             as filter
                                   from   dm_taxs.dealconversion dealconversion_1
                                          inner join
                                          dm_taxs.dealsecurity_act dealsecurity_act_2
                                             on dealconversion_1.deal_uk =
                                                   dealsecurity_act_2.deal_uk and
                                                dealsecurity_act_2.dealdirection_uk =
                                                   2
                                          left outer join
                                          dm_taxs.taxpair taxpair_3
                                             on dealsecurity_act_2.issuesecurity_uk =
                                                   taxpair_3.issuesecurity_remove_uk and
                                                dealsecurity_act_2.deal_uk =
                                                   taxpair_3.deal_remove_uk and
                                                taxpair_3.deleted_flag =
                                                   'N'
                                   where  (dealconversion_1.deleted_flag =
                                              'N') and
                                          (dealconversion_1.finished_flag =
                                              'N')
                                   group by dealconversion_1.link_ncode,
                                            dealsecurity_act_2.issuesecurity_uk,
                                            dealsecurity_act_2.deal_uk,
                                            dealsecurity_act_2.security_cnt)
                                  exp_pre_convdeal
                           where  (exp_pre_convdeal.filter = 0)) convdeal
                          inner join dm_taxs.taxpair taxpair_4
                             on convdeal.issuesecurity_uk =
                                   taxpair_4.issuesecurity_remove_uk and
                                convdeal.deal_uk =
                                   taxpair_4.deal_remove_uk and
                                taxpair_4.deleted_flag = 'N'
                          inner join dm_taxs.taxlot taxlot_5
                             on taxpair_4.taxlot_uk = taxlot_5.uk and
                                taxlot_5.deleted_flag = 'N'
                          inner join
                          dm_taxs.dealsecurity_act dealsecurity_act_6
                             on taxlot_5.deal_uk =
                                   dealsecurity_act_6.deal_uk and
                                taxlot_5.issuesecurity_uk =
                                   dealsecurity_act_6.issuesecurity_uk
                          inner join
                          main.exchangexrate exchangexrate_7
                             on exchangexrate_7.currency_to_uk =
                                   dealsecurity_act_6.currency_uk and
                                exchangexrate_7.value_day =
                                   taxlot_5.start_date and
                                exchangexrate_7.deleted_flag = 'N' and
                                exchangexrate_7.xratetype_uk = 1 and
                                exchangexrate_7.currency_from_uk =
                                   5205611685
                          inner join
                          main.exchangexrate exchangexrate_8
                             on exchangexrate_8.currency_to_uk =
                                   dealsecurity_act_6.currency_pay_uk and
                                exchangexrate_8.value_day =
                                   taxlot_5.start_date and
                                exchangexrate_8.deleted_flag = 'N' and
                                exchangexrate_8.xratetype_uk = 1 and
                                exchangexrate_8.currency_from_uk =
                                   5205611685
                          left outer join
                          dm_taxs.acctrevaluation2005_lstat acctrevaluation2005_lstat_9
                             on dealsecurity_act_6.deal_uk =
                                   acctrevaluation2005_lstat_9.deal_uk and
                                acctrevaluation2005_lstat_9.deleted_flag =
                                   'N'
                          left outer join
                          main.exchangexrate exchangexrate_10
                             on exchangexrate_10.currency_to_uk =
                                   acctrevaluation2005_lstat_9.currency_uk and
                                exchangexrate_10.value_day =
                                   taxlot_5.start_date and
                                exchangexrate_10.deleted_flag = 'N' and
                                exchangexrate_10.xratetype_uk = 1 and
                                exchangexrate_10.currency_from_uk =
                                   5205611685
                          left outer join
                          dm_taxs.dealoperacct_act dealoperacct_act_11
                             on dealsecurity_act_6.deal_uk =
                                   dealoperacct_act_11.deal_uk and
                                dealsecurity_act_6.issuesecurity_uk =
                                   dealoperacct_act_11.issuesecurity_uk and
                                dealoperacct_act_11.accountingevent_uk in (10,
                                                                                11,
                                                                                12,
                                                                                13)
                          inner join
                          main.exchangexrate exchangexrate_12
                             on exchangexrate_12.currency_to_uk =
                                   acctrevaluation2005_lstat_9.currency_uk and
                                exchangexrate_12.value_day =
                                   taxlot_5.start_date and
                                exchangexrate_12.deleted_flag = 'N' and
                                exchangexrate_12.xratetype_uk = 1 and
                                exchangexrate_12.currency_from_uk =
                                   5205611685
                   group by convdeal.link_ncode,
                            dealsecurity_act_6.security_deal_cnt,
                            taxpair_4.security_cnt) convsumrealiz
                  inner join
                  dm_taxs.dealconversion dealconversion_13
                     on convsumrealiz.link_ncode =
                           dealconversion_13.link_ncode and
                        dealconversion_13.deleted_flag = 'N' and
                        dealconversion_13.finished_flag = 'Y'
                  inner join
                  dm_taxs.dealsecurity_act dealsecurity_act_14
                     on dealconversion_13.deal_uk =
                           dealsecurity_act_14.deal_uk and
                        dealsecurity_act_14.dealdirection_uk = 1
                  inner join main.exchangexrate exchangexrate_15
                     on exchangexrate_15.currency_to_uk =
                           dealsecurity_act_14.currency_uk and
                        exchangexrate_15.value_day =
                           dealsecurity_act_14.reg_actual_date and
                        exchangexrate_15.deleted_flag = 'N' and
                        exchangexrate_15.xratetype_uk = 1 and
                        exchangexrate_15.currency_from_uk = 5205611685
                  inner join
                  dm_taxs.issuesecurity_act issuesecurity_act_16
                     on dealsecurity_act_14.issuesecurity_uk =
                           issuesecurity_act_16.uk) convpurchase_proc
------------------
merge into dm_taxs.dealconversionresult dealconversionresult
using  (select dealconversionresult_1.xk as xk
        from   dm_taxs.dealconversionresult dealconversionresult_1
               inner join dm_taxs.convpurchase_proc convpurchase_proc_2
                  on dealconversionresult_1.deal_uk =
                        convpurchase_proc_2.deal_uk and
                     dealconversionresult_1.issuesecurity_uk =
                        convpurchase_proc_2.issuesecurity_uk
        where  (dealconversionresult_1.deleted_flag = 'N')) exp_key
on     (dealconversionresult.xk = exp_key.xk)
when matched
then
   update set
      dealconversionresult.as_of_day      =
         to_date ('26-10-2015 13-56-26', 'DD-MM-YYYY HH24-MI-SS'),
      dealconversionresult.deleted_flag = 'Y',
      dealconversionresult.job_update   = 32098
------------------
insert into dm_taxs.dealconversionresult (value_day,
                                                xk,
                                                as_of_day,
                                                deleted_flag,
                                                emix,
                                                job_insert,
                                                job_update,
                                                deal_uk,
                                                issuesecurity_uk,
                                                currency_uk,
                                                deal_cur_amt,
                                                deal_couponaccount_cur_amt,
                                                currency_pay_uk,
                                                pay_amt,
                                                comission_rur_amt,
                                                revaluation_rur_amt)
   select convpurchase_proc_1.oper_date as value_day,
          dm_taxs.s_dealconversionresult.nextval as xk,
          to_date ('26-10-2015 13-56-26', 'DD-MM-YYYY HH24-MI-SS')
             as as_of_day,
          'N' as deleted_flag,
          235 as emix,
          32098 as job_insert,
          0 as job_update,
          convpurchase_proc_1.deal_uk as deal_uk,
          convpurchase_proc_1.issuesecurity_uk as issuesecurity_uk,
          convpurchase_proc_1.currency_uk as currency_uk,
          convpurchase_proc_1.deal_cur_amt as deal_cur_amt,
          convpurchase_proc_1.deal_couponaccount_cur_amt
             as deal_couponaccount_cur_amt,
          convpurchase_proc_1.currency_pay_uk as currency_pay_uk,
          convpurchase_proc_1.pay_cur_amt as pay_amt,
          convpurchase_proc_1.comission_rur_amt as comission_rur_amt,
          convpurchase_proc_1.revaluation_rur_amt as revaluation_rur_amt
   from   dm_taxs.convpurchase_proc convpurchase_proc_1
------------------
insert into dm_taxs.dealsecurity_mark (calcprice_old_amt,
                                             fiss_old_flag,
                                             pay_actual_old_date,
                                             pay_old_amt,
                                             pay_plan_old_date,
                                             pfi_old_ccode,
                                             receivedcoupon_cur_old_amt,
                                             reversal_old_description,
                                             calcprice_mark_amt,
                                             fiss_mark_flag,
                                             pay_actual_mark_date,
                                             pay_mark_amt,
                                             pay_plan_mark_date,
                                             pfi_mark_ccode,
                                             receivedcoupon_cur_mark_amt,
                                             reversal_mark_description,
                                             quotation_old_amt,
                                             quotation_mark_date,
                                             quotation_mark_rate,
                                             totaldeviation_old_amt,
                                             quotation_mark_amt,
                                             quotation_old_date,
                                             quotation_old_rate,
                                             totaldeviation_mark_amt,
                                             final_mark_flag,
                                             author_update_name,
                                             update_date,
                                             reg_actual_old_date,
                                             reg_actual_mark_date,
                                             value_day,
                                             pk,
                                             deleted_flag,
                                             deal_uk,
                                             deal_mark_ref,
                                             deal_old_ref,
                                             reg_plan_old_date,
                                             reg_plan_mark_date,
                                             issuesecurity_uk,
                                             security_old_cnt,
                                             deal_cur_old_amt,
                                             deal_couponacct_cur_old_amt,
                                             deal_couponacct_cur_mark_amt,
                                             deal_cur_mark_amt,
                                             dealfict_old_flag,
                                             security_mark_cnt,
                                             dealfict_mark_flag,
                                             dealrepayment_old_flag,
                                             dealrepayment_mark_flag,
                                             description,
                                             manual_mark_flag,
                                             currency_uk,
                                             currency_pay_uk,
                                             currency_issue_uk,
                                             dealemission_mark_flag,
                                             dealemission_old_flag,
                                             dealmark_mark_description,
                                             dealmark_old_description,
                                             trademode_quot_mark_uk,
                                             trademode_quot_old_uk,
                                             quotationsrc_old_uk,
                                             quotationsrc_mark_uk)
   select exp_res.calcprice_old_amt as calcprice_old_amt,
          exp_res.fiss_old_flag as fiss_old_flag,
          exp_res.pay_actual_old_date as pay_actual_old_date,
          exp_res.pay_old_amt as pay_old_amt,
          exp_res.pay_plan_old_date as pay_plan_old_date,
          exp_res.pfi_old_ccode as pfi_old_ccode,
          exp_res.receivedcoupon_cur_old_amt as receivedcoupon_cur_old_amt,
          exp_res.reversal_old_description as reversal_old_description,
          exp_res.calcprice_mark_amt as calcprice_mark_amt,
          exp_res.fiss_mark_flag as fiss_mark_flag,
          exp_res.pay_actual_mark_date as pay_actual_mark_date,
          exp_res.pay_mark_amt as pay_mark_amt,
          exp_res.pay_plan_mark_date as pay_plan_mark_date,
          exp_res.pfi_mark_ccode as pfi_mark_ccode,
          exp_res.receivedcoupon_cur_mark_amt as receivedcoupon_cur_mark_amt,
          exp_res.reversal_mark_description as reversal_mark_description,
          exp_res.quotation_old_amt as quotation_old_amt,
          exp_res.quotation_mark_date as quotation_mark_date,
          exp_res.quotation_mark_rate as quotation_mark_rate,
          exp_res.totaldeviation_old_amt as totaldeviation_old_amt,
          exp_res.quotation_mark_amt as quotation_mark_amt,
          exp_res.quotation_old_date as quotation_old_date,
          exp_res.quotation_old_rate as quotation_old_rate,
          exp_res.totaldeviation_mark_amt as totaldeviation_mark_amt,
          exp_res.final_mark_flag as final_mark_flag,
          exp_res.author_update_name as author_update_name,
          exp_res.update_date as update_date,
          exp_res.reg_actual_old_date as reg_actual_old_date,
          exp_res.reg_actual_mark_date as reg_actual_mark_date,
          exp_res.value_day as value_day,
          dm_taxs.s_dealsecurity_mark.nextval as pk,
          exp_res.deleted_flag as deleted_flag,
          exp_res.deal_uk as deal_uk,
          exp_res.deal_mark_ref as deal_mark_ref,
          exp_res.deal_old_ref as deal_old_ref,
          exp_res.reg_plan_old_date as reg_plan_old_date,
          exp_res.reg_plan_mark_date as reg_plan_mark_date,
          exp_res.issuesecurity_uk as issuesecurity_uk,
          exp_res.security_old_cnt as security_old_cnt,
          exp_res.deal_cur_old_amt as deal_cur_old_amt,
          exp_res.deal_couponacct_cur_old_amt as deal_couponacct_cur_old_amt,
          exp_res.deal_couponacct_cur_mark_amt
             as deal_couponacct_cur_mark_amt,
          exp_res.deal_cur_mark_amt as deal_cur_mark_amt,
          exp_res.dealfict_old_flag as dealfict_old_flag,
          exp_res.security_mark_cnt as security_mark_cnt,
          exp_res.dealfict_mark_flag as dealfict_mark_flag,
          exp_res.dealrepayment_old_flag as dealrepayment_old_flag,
          exp_res.dealrepayment_mark_flag as dealrepayment_mark_flag,
          exp_res.description as description,
          exp_res.manual_mark_flag as manual_mark_flag,
          exp_res.currency_uk as currency_uk,
          exp_res.currency_pay_uk as currency_pay_uk,
          exp_res.currency_issue_uk as currency_issue_uk,
          exp_res.dealemission_mark_flag as dealemission_mark_flag,
          exp_res.dealemission_old_flag as dealemission_old_flag,
          exp_res.dealmark_mark_description as dealmark_mark_description,
          exp_res.dealmark_old_description as dealmark_old_description,
          exp_res.trademode_quot_mark_uk as trademode_quot_mark_uk,
          exp_res.trademode_quot_old_uk as trademode_quot_old_uk,
          exp_res.quotationsrc_old_uk as quotationsrc_old_uk,
          exp_res.quotationsrc_mark_uk as quotationsrc_mark_uk
   from   (select 'akleyn' as author_update_name,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.calcprice_mark_amt
                  end
                     as calcprice_mark_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.calcprice_old_amt
                  end
                     as calcprice_old_amt,
                  convpurchase_proc_1.currency_issue_uk as currency_issue_uk,
                  convpurchase_proc_1.currency_pay_uk as currency_pay_uk,
                  convpurchase_proc_1.currency_uk as currency_uk,
                  convpurchase_proc_1.deal_couponaccount_cur_amt
                     as deal_couponacct_cur_mark_amt,
                  convpurchase_proc_1.deal_couponacct_cur_old_amt
                     as deal_couponacct_cur_old_amt,
                  convpurchase_proc_1.deal_cur_amt as deal_cur_mark_amt,
                  convpurchase_proc_1.deal_cur_old_amt as deal_cur_old_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.deal_mark_ref
                  end
                     as deal_mark_ref,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.deal_old_ref
                  end
                     as deal_old_ref,
                  convpurchase_proc_1.deal_uk as deal_uk,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealemission_mark_flag
                  end
                     as dealemission_mark_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealemission_old_flag
                  end
                     as dealemission_old_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealfict_mark_flag
                  end
                     as dealfict_mark_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealfict_old_flag
                  end
                     as dealfict_old_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealmark_mark_description
                  end
                     as dealmark_mark_description,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealmark_old_description
                  end
                     as dealmark_old_description,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealrepayment_mark_flag
                  end
                     as dealrepayment_mark_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.dealrepayment_old_flag
                  end
                     as dealrepayment_old_flag,
                  'N' as deleted_flag,
                  case
                     when union_tmp.tmp = 1
                     then
                        'Ручная маркировка'
                     else
                        'Финальная строка'
                  end
                     as description,
                  case when union_tmp.tmp = 1 then 'N' else 'Y' end
                     as final_mark_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.fiss_mark_flag
                  end
                     as fiss_mark_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.fiss_old_flag
                  end
                     as fiss_old_flag,
                  convpurchase_proc_1.issuesecurity_uk as issuesecurity_uk,
                  case when union_tmp.tmp = 1 then 'Y' else 'N' end
                     as manual_mark_flag,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.pay_actual_mark_date
                  end
                     as pay_actual_mark_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.pay_actual_old_date
                  end
                     as pay_actual_old_date,
                  convpurchase_proc_1.pay_cur_amt as pay_mark_amt,
                  convpurchase_proc_1.pay_old_amt as pay_old_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.pay_plan_mark_date
                  end
                     as pay_plan_mark_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.pay_plan_old_date
                  end
                     as pay_plan_old_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.pfi_mark_ccode
                  end
                     as pfi_mark_ccode,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.pfi_old_ccode
                  end
                     as pfi_old_ccode,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotation_mark_amt
                  end
                     as quotation_mark_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotation_mark_date
                  end
                     as quotation_mark_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotation_mark_rate
                  end
                     as quotation_mark_rate,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotation_old_amt
                  end
                     as quotation_old_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotation_old_date
                  end
                     as quotation_old_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotation_old_rate
                  end
                     as quotation_old_rate,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotationsrc_mark_uk
                  end
                     as quotationsrc_mark_uk,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.quotationsrc_old_uk
                  end
                     as quotationsrc_old_uk,
                  case
                     when union_tmp.tmp = 1
                     then
                        null
                     else
                        dealsecurity_mark_2.receivedcoupon_cur_mark_amt
                  end
                     as receivedcoupon_cur_mark_amt,
                  case
                     when union_tmp.tmp = 1
                     then
                        null
                     else
                        dealsecurity_mark_2.receivedcoupon_cur_old_amt
                  end
                     as receivedcoupon_cur_old_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.reg_actual_mark_date
                  end
                     as reg_actual_mark_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.reg_actual_old_date
                  end
                     as reg_actual_old_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.reg_plan_mark_date
                  end
                     as reg_plan_mark_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.reg_plan_old_date
                  end
                     as reg_plan_old_date,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.reversal_mark_description
                  end
                     as reversal_mark_description,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.reversal_old_description
                  end
                     as reversal_old_description,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.security_mark_cnt
                  end
                     as security_mark_cnt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.security_old_cnt
                  end
                     as security_old_cnt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.totaldeviation_mark_amt
                  end
                     as totaldeviation_mark_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.totaldeviation_old_amt
                  end
                     as totaldeviation_old_amt,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.trademode_quot_mark_uk
                  end
                     as trademode_quot_mark_uk,
                  case
                     when union_tmp.tmp = 1 then null
                     else dealsecurity_mark_2.trademode_quot_old_uk
                  end
                     as trademode_quot_old_uk,
                  sysdate as update_date,
                  sysdate as value_day
           from   dm_taxs.convpurchase_proc convpurchase_proc_1
                  left outer join
                  dm_taxs.dealsecurity_mark dealsecurity_mark_2
                     on convpurchase_proc_1.deal_uk =
                           dealsecurity_mark_2.deal_uk and
                        convpurchase_proc_1.issuesecurity_uk =
                           dealsecurity_mark_2.issuesecurity_uk and
                        dealsecurity_mark_2.deleted_flag = 'N' and
                        dealsecurity_mark_2.final_mark_flag = 'Y'
                  inner join
                  (select tmp as tmp
                   from   ( (select 1 as tmp from dual)
                           union all
                           (select 2 as tmp from dual)) union_tmp) union_tmp
                     on 1 = 1) exp_res
------------------
insert into dm_taxs.dealoperacct_mark (dealoperacct_start_date,
                                             author_update_name,
                                             update_date,
                                             dealoperacct_value_day,
                                             value_day,
                                             pk,
                                             deleted_flag,
                                             description,
                                             manual_mark_flag,
                                             final_mark_flag,
                                             issuesecurity_uk,
                                             oper_cur_mark_amt,
                                             accountingevent_uk,
                                             deal_uk,
                                             oper_cur_old_amt,
                                             paydocdirection_uk,
                                             security_uk,
                                             vat_cur_mark_amt,
                                             vat_cur_old_amt,
                                             currency_uk)
   select exp_res.dealoperacct_start_date as dealoperacct_start_date,
          exp_res.author_update_name as author_update_name,
          exp_res.update_date as update_date,
          exp_res.dealoperacct_value_day as dealoperacct_value_day,
          exp_res.value_day as value_day,
          dm_taxs.s_dealoperacct_mark.nextval as pk,
          exp_res.deleted_flag as deleted_flag,
          exp_res.description as description,
          exp_res.manual_mark_flag as manual_mark_flag,
          exp_res.final_mark_flag as final_mark_flag,
          exp_res.issuesecurity_uk as issuesecurity_uk,
          exp_res.oper_cur_mark_amt as oper_cur_mark_amt,
          exp_res.accountingevent_uk as accountingevent_uk,
          exp_res.deal_uk as deal_uk,
          exp_res.oper_cur_old_amt as oper_cur_old_amt,
          exp_res.paydocdirection_uk as paydocdirection_uk,
          exp_res.security_uk as security_uk,
          exp_res.vat_cur_mark_amt as vat_cur_mark_amt,
          exp_res.vat_cur_old_amt as vat_cur_old_amt,
          exp_res.currency_uk as currency_uk
   from   (select dealoperacct_act_2.accountingevent_uk
                     as accountingevent_uk,
                  'akleyn' as author_update_name,
                  dealoperacct_act_2.currency_uk as currency_uk,
                  dealoperacct_act_2.deal_uk as deal_uk,
                  dealoperacct_act_2.start_date
                     as dealoperacct_start_date,
                  dealoperacct_act_2.value_day as dealoperacct_value_day,
                  'N' as deleted_flag,
                  case
                     when union_tmp.tmp = 1
                     then
                        'Ручная маркировка'
                     else
                        'Финальная строка'
                  end
                     as description,
                  case when union_tmp.tmp = 1 then 'N' else 'Y' end
                     as final_mark_flag,
                  dealoperacct_act_2.issuesecurity_uk
                     as issuesecurity_uk,
                  case when union_tmp.tmp = 1 then 'Y' else 'N' end
                     as manual_mark_flag,
                  0 as oper_cur_mark_amt,
                  dealoperacct_act_2.oper_cur_amt as oper_cur_old_amt,
                  dealoperacct_act_2.paydocdirection_uk
                     as paydocdirection_uk,
                  dealoperacct_act_2.security_uk as security_uk,
                  sysdate as update_date,
                  sysdate as value_day,
                  0 as vat_cur_mark_amt,
                  dealoperacct_act_2.vat_cur_amt as vat_cur_old_amt
           from   dm_taxs.convpurchase_proc convpurchase_proc_1
                  inner join
                  dm_taxs.dealoperacct_act dealoperacct_act_2
                     on convpurchase_proc_1.deal_uk =
                           dealoperacct_act_2.deal_uk and
                        convpurchase_proc_1.issuesecurity_uk =
                           dealoperacct_act_2.issuesecurity_uk and
                        dealoperacct_act_2.oper_cur_amt > 0 or
                        dealoperacct_act_2.vat_cur_amt > 0
                  left outer join
                  dm_taxs.dealoperacct_mark dealoperacct_mark_3
                     on dealoperacct_mark_3.accountingevent_uk =
                           dealoperacct_act_2.accountingevent_uk and
                        dealoperacct_mark_3.deal_uk =
                           dealoperacct_act_2.deal_uk and
                        dealoperacct_mark_3.issuesecurity_uk =
                           dealoperacct_act_2.issuesecurity_uk and
                        dealoperacct_mark_3.paydocdirection_uk =
                           dealoperacct_act_2.paydocdirection_uk and
                        dealoperacct_mark_3.security_uk =
                           dealoperacct_act_2.security_uk and
                        dealoperacct_mark_3.value_day =
                           dealoperacct_act_2.value_day and
                        dealoperacct_mark_3.deleted_flag = 'N' and
                        dealoperacct_mark_3.final_mark_flag = 'Y'
                  inner join
                  (select tmp as tmp
                   from   ( (select 1 as tmp from dual)
                           union all
                           (select 2 as tmp from dual)) union_tmp) union_tmp
                     on 1 = 1) exp_res
------------------
insert into dm_taxs.eventrecalculation (xk)
   select dm_taxs.s_eventrecalculation.nextval as xk
   from   (select to_date ('26-10-2015 13-56-26', 'DD-MM-YYYY HH24-MI-SS')
                     as as_of_day,
                  'N' as deleted_flag,
                  235 as emix,
                  case
                     when union_tmp.tmp = 6
                     then
                        to_char (
                           convpurchase_proc_1.deal_couponaccount_cur_amt)
                     when union_tmp.tmp = 7
                     then
                        to_char (convpurchase_proc_1.deal_cur_amt)
                     when union_tmp.tmp = 24
                     then
                        to_char (convpurchase_proc_1.pay_cur_amt)
                     else
                        null
                  end
                     as end_value,
                  sysdate as event_time,
                  union_tmp.tmp as eventattribute_uk,
                  1 as eventtable_uk,
                  case
                     when convpurchase_proc_1.reg_actual_date <
                             to_date ('20141231', 'yyyyMMdd')
                     then
                        'Y'
                     else
                        'N'
                  end
                     as fixedperiod_flag,
                  32098 as job_insert,
                  0 as job_update,
                  'Y' as manual_event_flag,
                  convpurchase_proc_1.deal_uk ||
                  '~' ||
                  convpurchase_proc_1.issuesecurity_uk
                     as src_ccode,
                  'SMART ID – ' ||
                  convpurchase_proc_1.deal_back_ref ||
                  ' ISIN – ' ||
                  convpurchase_proc_1.isin ||
                  '  Номер/Серия – ' ||
                  convpurchase_proc_1.issuesecurity_series ||
                  ' Конвертация'
                     as src_description,
                  case
                     when union_tmp.tmp = 6
                     then
                        to_char (
                           convpurchase_proc_1.deal_couponacct_cur_old_amt)
                     when union_tmp.tmp = 7
                     then
                        to_char (convpurchase_proc_1.deal_cur_old_amt)
                     when union_tmp.tmp = 24
                     then
                        to_char (convpurchase_proc_1.pay_old_amt)
                     else
                        null
                  end
                     as start_value,
                  trunc (sysdate, 'DD') as value_day
           from   dm_taxs.convpurchase_proc convpurchase_proc_1
                  inner join
                  (select tmp as tmp
                   from   ( (select 6 as tmp from dual)
                           union all
                           (select 7 as tmp from dual)
                           union all
                           (select 24 as tmp from dual)) union_tmp) union_tmp
                     on 1 = 1) exp_res
 -----------------
insert into dm_taxs.eventrecalculation (xk)
   select dm_taxs.s_eventrecalculation.nextval as xk
   from   (select to_date ('26-10-2015 13-56-26', 'DD-MM-YYYY HH24-MI-SS')
                     as as_of_day,
                  'N' as deleted_flag,
                  235 as emix,
                  0 as end_value,
                  sysdate as event_time,
                  union_tmp.tmp as eventattribute_uk,
                  8 as eventtable_uk,
                  case
                     when convpurchase_proc_1.reg_actual_date <
                             to_date ('20141231', 'yyyyMMdd')
                     then
                        'Y'
                     else
                        'N'
                  end
                     as fixedperiod_flag,
                  32098 as job_insert,
                  0 as job_update,
                  'Y' as manual_event_flag,
                  dealoperacct_act_2.tk as src_ccode,
                  'SMART ID – ' ||
                  convpurchase_proc_1.deal_back_ref ||
                  ' ISIN – ' ||
                  convpurchase_proc_1.isin ||
                  '  Номер/Серия – ' ||
                  convpurchase_proc_1.issuesecurity_series ||
                  ' Операция – ' ||
                  accountingevent_3.name ||
                  ' Дата – ' ||
                  dealoperacct_act_2.value_day ||
                  ' Конвертация'
                     as src_description,
                  case
                     when union_tmp.tmp = 100
                     then
                        to_char (dealoperacct_act_2.oper_cur_amt)
                     when union_tmp.tmp = 105
                     then
                        to_char (dealoperacct_act_2.vat_cur_amt)
                     else
                        null
                  end
                     as start_value,
                  trunc (sysdate, 'DD') as value_day
           from   dm_taxs.convpurchase_proc convpurchase_proc_1
                  inner join
                  dm_taxs.dealoperacct_act dealoperacct_act_2
                     on convpurchase_proc_1.deal_uk =
                           dealoperacct_act_2.deal_uk and
                        convpurchase_proc_1.issuesecurity_uk =
                           dealoperacct_act_2.issuesecurity_uk and
                        dealoperacct_act_2.oper_cur_amt > 0 or
                        dealoperacct_act_2.vat_cur_amt > 0
                  inner join dm_01.accountingevent accountingevent_3
                     on dealoperacct_act_2.accountingevent_uk =
                           accountingevent_3.uk
                  inner join
                  (select tmp as tmp
                   from   ( (select 100 as tmp from dual)
                           union all
                           (select 105 as tmp from dual)) union_tmp)
                  union_tmp
                     on 1 = 1) exp_res
------------------
merge into dm_taxs.dealsecurity_act dealsecurity_act
using  (select to_date ('26-10-2015 13-56-26', 'DD-MM-YYYY HH24-MI-SS')
                  as as_of_day,
               convpurchase_proc_1.deal_cur_amt -
               convpurchase_proc_1.deal_couponaccount_cur_amt
                  as cost_cur_amt,
               convpurchase_proc_1.deal_couponaccount_cur_amt
                  as deal_couponaccount_cur_amt,
               convpurchase_proc_1.deal_cur_amt as deal_cur_amt,
               32098 as job_update,
               convpurchase_proc_1.pay_cur_amt as pay_amt,
               decode (
                  convpurchase_proc_1.security_cnt,
                  0, 0,
                  (convpurchase_proc_1.deal_cur_amt -
                   convpurchase_proc_1.deal_couponaccount_cur_amt) /
                  convpurchase_proc_1.security_cnt)
                  as price_cur_amt,
               convpurchase_proc_1.deal_uk as deal_uk,
               convpurchase_proc_1.issuesecurity_uk as issuesecurity_uk
        from   dm_taxs.convpurchase_proc convpurchase_proc_1) exp_res
on     (exp_res.deal_uk = dealsecurity_act.deal_uk and
        exp_res.issuesecurity_uk = dealsecurity_act.issuesecurity_uk)
when matched
then
   update set
      dealsecurity_act.deal_cur_amt  = exp_res.deal_cur_amt,
      dealsecurity_act.deal_couponaccount_cur_amt      =
         exp_res.deal_couponaccount_cur_amt,
      dealsecurity_act.as_of_day     = exp_res.as_of_day,
      dealsecurity_act.job_update    = exp_res.job_update,
      dealsecurity_act.cost_cur_amt  = exp_res.cost_cur_amt,
      dealsecurity_act.price_cur_amt = exp_res.price_cur_amt,
      dealsecurity_act.pay_amt       = exp_res.pay_amt
------------------
merge into dm_taxs.dealoperacct_act dealoperacct_act
using  (select convpurchase_proc_1.deal_uk as deal_uk,
               convpurchase_proc_1.issuesecurity_uk as issuesecurity_uk
        from   dm_taxs.convpurchase_proc convpurchase_proc_1) exp_key
on     (exp_key.deal_uk = dealoperacct_act.deal_uk and
        exp_key.issuesecurity_uk = dealoperacct_act.issuesecurity_uk)
when matched
then
   update set
      dealoperacct_act.vat_cur_amt        = 0,
      dealoperacct_act.opernotvat_cur_amt = 0,
      dealoperacct_act.oper_cur_amt       = 0
------------------
merge into dm_taxs.dealconversion dealconversion
using  (select dealconversion_2.pk as pk
        from   dm_taxs.dealconversion dealconversion_2
        where  (dealconversion_2.deleted_flag = 'N') and
               (dealconversion_2.finished_flag = 'N') and
               (dealconversion_2.link_ncode in ( (select distinct
                                                               convpurchase_proc_1.link_ncode
                                                                  as link_ncode
                                                        from   dm_taxs.convpurchase_proc convpurchase_proc_1))))
       exp_key
on     (exp_key.pk = dealconversion.pk)
when matched
then
   update set
      dealconversion.finished_flag      = 'Y',
      dealconversion.author_update_name = 'akleyn',
      dealconversion.update_date        =
         to_date ('26-10-2015 13-56-26', 'DD-MM-YYYY HH24-MI-SS')
