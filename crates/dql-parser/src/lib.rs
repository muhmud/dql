use std::collections::HashMap;

use chrono::{DateTime, Utc};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1, digit1, multispace0};
use nom::combinator::{map, map_res, opt, recognize};
use nom::multi::{many0_count, separated_list1};
use nom::sequence::{delimited, pair, separated_pair};
use nom::IResult;
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug)]
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

#[derive(Debug, PartialEq)]
pub enum PathElement {
    Named(String),
    Wildcard,
}

pub struct Path {
    pub data_source: Option<String>,
    pub elements: Vec<PathElement>,
}

#[derive(Debug)]
pub enum PredicateComponentType {
    Current,
    Literal(Data),
}

#[derive(Debug)]
pub enum PredicateComparisonType {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
}

// name[== "abc"]
// id[? > 23]
#[derive(Debug)]
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

pub fn undelimited_identifier(input: &str) -> IResult<&str, &str> {
    ws(recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_"), tag("-")))),
    )))(input)
}

pub fn delimited_identifier(input: &str) -> IResult<&str, &str> {
    ws(delimited(tag("`"), undelimited_identifier, tag("`")))(input)
}

pub fn identifier(input: &str) -> IResult<&str, &str> {
    alt((undelimited_identifier, delimited_identifier))(input)
}

pub fn int(input: &str) -> IResult<&str, i64> {
    map_res(digit1, |s: &str| s.parse::<i64>())(input)
}

pub fn range_path_range(input: &str) -> IResult<&str, (Option<i64>, Option<i64>)> {
    separated_pair(opt(int), tag(":"), opt(int))(input)
}

pub fn range_path_index(input: &str) -> IResult<&str, i64> {
    int(input)
}

pub fn path(input: &str) -> IResult<&str, Path> {
    let (input, data_source) = opt(nom::sequence::terminated(identifier, tag("::")))(input)?;
    map(
        map(separated_list1(tag("/"), identifier), |elements| {
            elements
                .iter()
                .map(ToString::to_string)
                .map(PathElement::Named)
                .collect()
        }),
        move |elements| Path {
            data_source: data_source.map(|s| s.to_string()),
            elements,
        },
    )(input)
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
            range_path_range("123:456"),
            Ok(("", (Some(123), Some(456))))
        );
        assert_eq!(range_path_range("1:"), Ok(("", (Some(1), None))));
        assert_eq!(range_path_range(":2"), Ok(("", (None, Some(2)))));
    }

    #[test]
    fn parsing_path_works_correctly() {
        if let Ok((_, parsed_path)) = path("customer::name") {
            assert_eq!(parsed_path.data_source.unwrap(), "customer");
            assert_eq!(parsed_path.elements.len(), 1);
            assert_eq!(
                parsed_path.elements[0],
                PathElement::Named(str::to_string("name"))
            );
        } else {
            panic!("could not parse path");
        }

        if let Ok((_, parsed_path)) = path("customer::details/name") {
            assert_eq!(parsed_path.data_source.unwrap(), "customer");
            assert_eq!(parsed_path.elements.len(), 2);
            assert_eq!(
                parsed_path.elements[0],
                PathElement::Named(str::to_string("details"))
            );
            assert_eq!(
                parsed_path.elements[1],
                PathElement::Named(str::to_string("name"))
            );
        } else {
            panic!("could not parse path");
        }

        if let Ok((_, parsed_path)) = path("details/name") {
            assert_eq!(parsed_path.data_source, None);
            assert_eq!(parsed_path.elements.len(), 2);
            assert_eq!(
                parsed_path.elements[0],
                PathElement::Named(str::to_string("details"))
            );
            assert_eq!(
                parsed_path.elements[1],
                PathElement::Named(str::to_string("name"))
            );
        } else {
            panic!("could not parse path");
        }
    }
}
