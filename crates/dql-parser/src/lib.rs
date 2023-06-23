use std::collections::HashMap;

use chrono::{DateTime, Utc};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1, digit1, multispace0};
use nom::combinator::{map_res, opt, recognize};
use nom::error::ParseError;
use nom::multi::many0_count;
use nom::sequence::{delimited, pair, separated_pair, tuple};
use nom::IResult;
use rust_decimal::Decimal;
use uuid::Uuid;

pub enum Data {
    Null,

    // Scalar data types
    String(String),
    Float(f64),
    Decimal(Decimal),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    Int(i32),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    UInt(u32),
    Bool(bool),
    UUID(Uuid),
    Binary(Box<Vec<u8>>),
    DateTime(DateTime<Utc>),
    Date(DateTime<Utc>),
    Time(DateTime<Utc>),

    // Composite data types
    Object {
        field_order: Box<Vec<String>>,
        values: Box<HashMap<String, Data>>,
    },
    Array(Box<Vec<Data>>),
}

pub fn is_scalar(data: &Data) -> bool {
    !is_object(data) && !is_array(data)
}

pub fn is_object(data: &Data) -> bool {
    matches!(
        data,
        Data::Object {
            field_order: _,
            values: _
        }
    )
}

pub fn is_array(data: &Data) -> bool {
    matches!(data, Data::Array(_))
}

pub fn is_null(data: &Data) -> bool {
    matches!(data, Data::Null)
}

pub enum Token {
    Identifier(String),            // A data source name, field name, etc.
    ResolutionOperator,            // ::
    PathTraversalOperator,         // /
    FilterStartOperator,           // [
    FilterEndOperator,             // ]
    ProjectionOperator,            // ->
    ObjectProjectionStartOperator, // {
    ObjectProjectionEndOperator,   // }
    ArrayProjectionStartOperator,  // [
    ArrayProjectionEndOperator,    // ]
    ObjectElementOperator,         // :
    ElementSeparatorOperator,      // ,
}

pub trait BoolExpression {
    fn evaluate(&self, data: &Data) -> bool;
}

pub struct DataIterator {
    pub data: Data,
}

impl Iterator for DataIterator {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

pub trait DataExpression {
    fn evaluate(&self) -> Data;
}

pub struct ObjectQuery {
    pub fields: Option<HashMap<String, Box<dyn DataExpression>>>,
    pub grouping: Option<Vec<Box<dyn DataExpression>>>,
    pub order: Option<Vec<Box<dyn DataExpression>>>,
}

pub struct ArrayQuery {
    pub values: Vec<Box<dyn DataExpression>>,
    pub order: Vec<Box<dyn DataExpression>>,
}

pub trait DataSource {
    fn data(&self, filter: Option<Box<dyn BoolExpression>>) -> DataIterator;
    fn join(
        &self,
        data_source: Box<dyn DataSource>,
        condition: Box<dyn BoolExpression>,
    ) -> Box<dyn DataSource>;
}

pub trait DataStore {
    fn name(&self) -> &str;
    fn get_data_source(&self, name: &str) -> Box<dyn DataSource>;
}

pub struct DataEnvironment {
    pub data_stores: HashMap<String, Box<dyn DataStore>>,
}

pub enum PathElement {
    Named(String),
    Wildcard,
}

pub enum PredicateComponentType {
    Current,
    Literal(Data),
}

pub enum PredicateComparisonType {
    Equals,
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
}

// name[== "abc"]
// id[? > 23]
pub enum PredicateExpression {
    Or(Box<PredicateExpression>, Box<PredicateExpression>),
    And(Box<PredicateExpression>, Box<PredicateExpression>),
    Not(Box<PredicateExpression>),
    Predicate(
        PredicateComponentType,
        PredicateComparisonType,
        PredicateComponentType,
    ),
}

pub enum PathFilter {
    Predicate(PredicateExpression),
    Range(Option<i64>, Option<i64>),
    Index(i64),
}

pub struct PathComponent {
    pub element: PathElement,
    pub filter: Option<PathFilter>,
}

pub enum QueryValue {
    Field,
    Subquery(Query),
    Literal(Data),
    Path(Box<Vec<PathComponent>>),
}

pub enum Query {
    Object(Box<Vec<(String, QueryValue)>>),
    Array(Box<Vec<QueryValue>>),
}

pub struct QueryRequest {
    pub data_store: Option<String>,
    pub query: Query,
}

pub fn ws<'a, F, O>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    delimited(multispace0, inner, multispace0)
}

pub fn identifier(input: &str) -> IResult<&str, &str> {
    ws(recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_"), tag("-")))),
    )))(input)
}

pub fn delimited_identifier(input: &str) -> IResult<&str, &str> {
    ws(delimited(tag("`"), identifier, tag("`")))(input)
}

pub fn int(input: &str) -> IResult<&str, i64> {
    map_res(digit1, |s: &str| s.parse::<i64>())(input)
}

pub fn range_path_filter(input: &str) -> IResult<&str, (Option<i64>, Option<i64>)> {
    separated_pair(opt(int), tag(":"), opt(int))(input)
}

pub fn range_path_index(input: &str) -> IResult<&str, i64> {
    int(input)
}

pub fn object_query(input: &str) -> IResult<&str, Query> {
    let (mut input, _) = ws(tag("{"))(input)?;
    let mut fields: Vec<(String, QueryValue)> = vec![];

    if let Ok((i, (_, _, _))) = tuple((opt(ws(tag("*"))), opt(ws(tag(","))), ws(tag("}"))))(input) {
        return Ok((i, Query::Object(Box::new(fields))));
    }

    loop {
        if let Ok((i, _)) = ws(tag("}"))(input) {
            return Ok((i, Query::Object(Box::new(fields))));
        }
        if let Ok((i, (field_name, _))) = tuple((ws(identifier), opt(ws(tag(",")))))(input) {
            let path = vec![PathComponent {
                element: PathElement::Named(field_name.to_owned()),
                filter: None,
            }];
            fields.push((field_name.to_owned(), QueryValue::Path(Box::new(path))));
            input = i;
        } else if let Ok((i, field_name)) = tuple((ws(identifier), ws(tag(":")), ws(tag("\""))))
    }
}

pub fn parse_query(input: &str) -> IResult<&str, QueryRequest> {
    let (input, data_store) = alt((opt(identifier), opt(delimited_identifier)))(input)?;
    let vec: Vec<(String, QueryValue)> = vec![];

    Ok((
        input,
        QueryRequest {
            data_store: data_store.map(ToOwned::to_owned),
            query: Query::Object(Box::new(vec)),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing_identifier_works_correctly() {
        assert_eq!(identifier("customer"), Ok(("", "customer")));
        assert_eq!(delimited_identifier("`customer`"), Ok(("", "customer")));
        assert_eq!(identifier("customer::"), Ok(("::", "customer")));
    }

    #[test]
    fn parsing_int_works_correctly() {
        assert_eq!(int("123"), Ok(("", 123)));
    }

    #[test]
    fn parsing_range_filter_works_correctly() {
        assert_eq!(
            range_path_filter("123:456"),
            Ok(("", (Some(123), Some(456))))
        );
        assert_eq!(range_path_filter("1:"), Ok(("", (Some(1), None))));
        assert_eq!(range_path_filter(":2"), Ok(("", (None, Some(2)))));
    }
}
