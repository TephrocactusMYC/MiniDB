use super::OpIterator;
use crate::Managers;
use common::bytecode_expr::ByteCodeExpr;
use common::datatypes::f_decimal;
use common::{AggOp, CrustyError, Field, TableSchema, Tuple};
use std::collections::HashMap;

#[derive(Clone, Debug)]
enum AggregateState {
    Count(i64),
    Sum(Option<Field>),
    Min(Option<Field>),
    Max(Option<Field>),
    Avg { sum: Option<Field>, count: i64 },
}

pub struct Aggregate {
    managers: &'static Managers,
    schema: TableSchema,
    groupby_expr: Vec<ByteCodeExpr>,
    agg_expr: Vec<ByteCodeExpr>,
    ops: Vec<AggOp>,
    child: Box<dyn OpIterator>,
    will_rewind: bool,
    groups: HashMap<Vec<Field>, Vec<AggregateState>>,
    result_buffer: Option<Vec<Tuple>>,
    buffer_iterator_idx: usize,
    open: bool,
}

impl Aggregate {
    pub fn new(
        managers: &'static Managers,
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
        schema: TableSchema,
        child: Box<dyn OpIterator>,
    ) -> Self {
        assert!(ops.len() == agg_expr.len());
        Self {
            managers,
            schema,
            groupby_expr,
            agg_expr,
            ops,
            child,
            will_rewind: false,
            groups: HashMap::new(),
            result_buffer: None,
            buffer_iterator_idx: 0,
            open: false,
        }
    }

    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) -> Result<(), CrustyError> {
        let mut key = Vec::with_capacity(self.groupby_expr.len());
        for expr in &self.groupby_expr {
            key.push(expr.eval(tuple));
        }
        let values: Vec<Field> = self.agg_expr.iter().map(|e| e.eval(tuple)).collect();
        let states = self.groups.entry(key.clone()).or_insert_with(|| {
            self.ops
                .iter()
                .map(|op| match op {
                    AggOp::Count => AggregateState::Count(0),
                    AggOp::Sum => AggregateState::Sum(None),
                    AggOp::Min => AggregateState::Min(None),
                    AggOp::Max => AggregateState::Max(None),
                    AggOp::Avg => AggregateState::Avg {
                        sum: None,
                        count: 0,
                    },
                })
                .collect()
        });
        for (i, op) in self.ops.iter().enumerate() {
            let v = &values[i];
            match (&mut states[i], op) {
                (AggregateState::Count(cnt), AggOp::Count) => {
                    if *v != Field::Null {
                        *cnt += 1;
                    }
                }
                (AggregateState::Sum(opt), AggOp::Sum) => {
                    if *v != Field::Null {
                        if let Some(prev) = opt.take() {
                            *opt = Some((prev + v.clone())?);
                        } else {
                            *opt = Some(v.clone());
                        }
                    }
                }
                (AggregateState::Min(opt), AggOp::Min) => {
                    if *v != Field::Null {
                        if let Some(prev) = opt {
                            if v < prev {
                                *opt = Some(v.clone());
                            }
                        } else {
                            *opt = Some(v.clone());
                        }
                    }
                }
                (AggregateState::Max(opt), AggOp::Max) => {
                    if *v != Field::Null {
                        if let Some(prev) = opt {
                            if v > prev {
                                *opt = Some(v.clone());
                            }
                        } else {
                            *opt = Some(v.clone());
                        }
                    }
                }
                (AggregateState::Avg { sum, count }, AggOp::Avg) => {
                    if *v != Field::Null {
                        if let Some(prev) = sum.take() {
                            *sum = Some((prev + v.clone())?);
                        } else {
                            *sum = Some(v.clone());
                        }
                        *count += 1;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn finalize_group(
        key: Vec<Field>,
        states: Vec<AggregateState>,
        ops: &[AggOp],
    ) -> Result<Tuple, CrustyError> {
        let mut row = key;
        for (state, op) in states.into_iter().zip(ops.iter()) {
            match (state, op) {
                (AggregateState::Count(cnt), AggOp::Count) => row.push(Field::Int(cnt)),
                (AggregateState::Sum(opt), AggOp::Sum)
                | (AggregateState::Min(opt), AggOp::Min)
                | (AggregateState::Max(opt), AggOp::Max) => {
                    row.push(opt.unwrap_or(Field::Null));
                }
                (AggregateState::Avg { sum, count }, AggOp::Avg) => {
                    if count == 0 {
                        row.push(Field::Null);
                    } else if let Some(sf) = sum {
                        let sum_f = match sf {
                            Field::Int(i) => Some(i as f64),
                            Field::Decimal(v, s) if s <= 308 => {
                                Some(v as f64 / 10f64.powi(s as i32))
                            }
                            _ => None,
                        };
                        if let Some(sv) = sum_f {
                            row.push(f_decimal(sv / count as f64));
                        } else {
                            return Err(CrustyError::ExecutionError("AVG non-numeric".into()));
                        }
                    } else {
                        row.push(Field::Null);
                    }
                }
                _ => return Err(CrustyError::CrustyError("State/op mismatch".into())),
            }
        }
        Ok(Tuple::new(row))
    }

    fn produce_results(&mut self) -> Result<(), CrustyError> {
        let mut buf = Vec::with_capacity(self.groups.len());
        for (key, states) in self.groups.drain() {
            buf.push(Self::finalize_group(key, states, &self.ops)?);
        }
        self.result_buffer = Some(buf);
        self.buffer_iterator_idx = 0;
        Ok(())
    }
}

impl OpIterator for Aggregate {
    fn configure(&mut self, will_rewind: bool) {
        self.will_rewind = will_rewind;
        self.child.configure(false);
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        self.child.open()?;
        while let Some(t) = self.child.next()? {
            self.merge_tuple_into_group(&t)?;
        }
        self.child.close()?;
        self.produce_results()?;
        self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("next() called before open()");
        }
        if !self.will_rewind {
            match self.result_buffer.as_ref() {
                Some(buf) if self.buffer_iterator_idx < buf.len() => {
                    let t = buf[self.buffer_iterator_idx].clone();
                    self.buffer_iterator_idx += 1;
                    Ok(Some(t))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("close() called before open()");
        }
        self.open = false;
        self.result_buffer = None;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("rewind() called before open()");
        }
        self.buffer_iterator_idx = 0;
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::testutil::{execute_iter, new_test_managers, TestTuples};
    use common::{
        bytecode_expr::colidx_expr,
        datatypes::{f_int, f_str},
    };

    fn get_iter(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = new_test_managers();
        let dummy_schema = TableSchema::new(vec![]);
        let mut iter = Box::new(Aggregate::new(
            managers,
            groupby_expr,
            agg_expr,
            ops,
            dummy_schema,
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn run_aggregate(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Vec<Tuple> {
        let mut iter = get_iter(groupby_expr, agg_expr, ops);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod aggregation_test {
        use super::*;

        #[test]
        fn test_empty_group() {
            let group_by = vec![];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 1);
            assert_eq!(t[0], Tuple::new(vec![f_int(6), f_int(2), f_decimal(4.0)]));
        }

        #[test]
        fn test_empty_aggregation() {
            let group_by = vec![colidx_expr(2)];
            let agg = vec![];
            let ops = vec![];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 3);
            assert_eq!(t[0], Tuple::new(vec![f_int(3)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(4)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(5)]));
        }

        #[test]
        fn test_count() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Count];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 2
            // 1 4 1
            // 2 4 1
            // 2 5 2
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_int(2)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_int(1)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_int(1)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_int(2)]));
        }

        #[test]
        fn test_sum() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Sum];
            let tuples = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 3
            // 1 4 3
            // 2 4 4
            // 2 5 11
            assert_eq!(tuples.len(), 4);
            assert_eq!(tuples[0], Tuple::new(vec![f_int(1), f_int(3), f_int(3)]));
            assert_eq!(tuples[1], Tuple::new(vec![f_int(1), f_int(4), f_int(3)]));
            assert_eq!(tuples[2], Tuple::new(vec![f_int(2), f_int(4), f_int(4)]));
            assert_eq!(tuples[3], Tuple::new(vec![f_int(2), f_int(5), f_int(11)]));
        }

        #[test]
        fn test_max() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Max];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 G
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("G")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_min() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Min];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 E
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert!(t.len() == 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("E")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_avg() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 1.5
            // 1 4 3.0
            // 2 4 4.0
            // 2 5 5.5
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_decimal(1.5)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_decimal(3.0)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_decimal(4.0)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_decimal(5.5)]));
        }

        #[test]
        fn test_multi_column_aggregation() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(3)];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // A 1 1 4.0
            // E 1 1 3.0
            // G 4 2 4.25
            assert_eq!(t.len(), 3);
            assert_eq!(
                t[0],
                Tuple::new(vec![f_str("A"), f_int(1), f_int(1), f_decimal(4.0)])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![f_str("E"), f_int(1), f_int(1), f_decimal(3.0)])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![f_str("G"), f_int(4), f_int(2), f_decimal(4.25)])
            );
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let group_by = vec![];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Avg];
            let _ = run_aggregate(group_by, agg, ops);
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter(vec![colidx_expr(2)], vec![colidx_expr(0)], vec![AggOp::Max]);
            iter.configure(true); // if we will rewind in the future, then we set will_rewind to true
            let t_before = execute_iter(&mut *iter, true).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, true).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
